"""Cron-driven cache builder.

Runs in GitHub Actions every 30 min:
  1. Fetches Smartlead campaign + lead data
  2. Fetches AimFox LinkedIn data
  3. Writes everything to data/cache.json
  4. Workflow commits + pushes to data-cache branch

The Render dashboard reads cache.json from raw.githubusercontent.com
and never calls Smartlead/AimFox itself.

Run locally:  python sync_cache.py
"""
import gzip
import json
import logging
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from subsequence_analytics import (
    get_all_campaigns, get_campaign_stats, collect_leads_detail,
    compute_analytics, enrich_replied_leads, resolve_client,
)

try:
    from aimfox_client import AimfoxClient
    AIMFOX_AVAILABLE = True
except Exception as e:
    print(f"[warn] aimfox unavailable: {e}")
    AIMFOX_AVAILABLE = False


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# Smartlead rate limit is ~60 req/min; with retry-on-429 backoff we can
# stay close to 2 workers without losing data. Tune via env var.
FETCH_WORKERS = int(os.getenv("SYNC_WORKERS", "2"))
OUTPUT_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
# We write a gzipped JSON so the resulting file fits within GitHub's
# 100 MB single-file limit (raw JSON is ~120-150 MB at scale).
OUTPUT_PATH    = os.path.join(OUTPUT_DIR, "cache.json")       # uncompressed (debug only)
OUTPUT_GZ_PATH = os.path.join(OUTPUT_DIR, "cache.json.gz")    # served to dashboard


# Cap on stored content per lead — keeps the JSON manageable.
MAX_MSG_BODY_CHARS = 400
MAX_MSGS_PER_LEAD  = 8


def slim_lead(lead):
    """Drop bulky/unused fields. Normalise message direction so the
    chat-thread renderer can color-code sent vs received.
    Smartlead emits {is_reply, sent_time, body, subject}; AimFox emits
    {type, from, time, body}. Output is unified: {type, from, subject,
    time, body}."""
    msgs = lead.get("messages") or []
    lead_email = lead.get("email", "")
    lead_name  = lead.get("name") or lead_email or "Unknown"

    def _slim_msg(m):
        is_reply = m.get("is_reply")
        if is_reply is None:
            t = (m.get("type") or m.get("direction") or "").lower()
            is_reply = t in ("inbound", "received", "reply", "replied")
        return {
            "type":    "inbound" if is_reply else "outbound",
            "from":    (m.get("from") or (lead_name if is_reply else "Us"))[:80],
            "subject": (m.get("subject") or "")[:200],
            "time":    (m.get("time") or m.get("sent_time")
                        or m.get("received_at") or "")[:19],
            "body":    (m.get("body") or "")[:MAX_MSG_BODY_CHARS],
        }

    return {
        "name":      lead_name,
        "email":     lead_email,
        "category":  lead.get("category", ""),
        "company":   lead.get("company", ""),
        "sent_time":  (lead.get("sent_time")  or "")[:19],
        "open_time":  (lead.get("open_time")  or "")[:19],
        "click_time": (lead.get("click_time") or "")[:19],
        "reply_time": (lead.get("reply_time") or "")[:19],
        "messages": [_slim_msg(m) for m in msgs[:MAX_MSGS_PER_LEAD]],
    }


def slim_campaign(camp):
    """Slim a parent or sub campaign row in-place without losing aggregates."""
    leads = camp.get("leads") or []
    camp["leads"] = [slim_lead(l) for l in leads]
    return camp


def _fetch_sub(sub, parent_map):
    parent      = parent_map.get(sub["parent_campaign_id"], {})
    parent_name = resolve_client(parent.get("name", f"ID:{sub['parent_campaign_id']}"))
    stats       = get_campaign_stats(sub["id"])
    leads       = collect_leads_detail(stats)
    a           = compute_analytics(leads)
    emails      = {l["email"] for l in leads}
    return {
        "pid":    sub["parent_campaign_id"],
        "row":    {"parent": parent_name, "subsequence": sub["name"],
                   "status": sub.get("status", "UNKNOWN"), **a, "leads": leads},
        "emails": emails,
    }


def _fetch_parent(pid, parent, sub_emails):
    stats       = get_campaign_stats(pid)
    leads       = collect_leads_detail(stats)
    a           = compute_analytics(leads)
    leads       = enrich_replied_leads(pid, leads)
    parent_emails = {l["email"] for l in leads}
    added         = len(sub_emails.get(pid, set()) & parent_emails)
    client_name   = resolve_client(parent.get("name", f"ID:{pid}"))
    return {
        "client": client_name, "raw_name": parent.get("name", ""),
        "status": parent.get("status", "UNKNOWN"), **a,
        "added_to_sub":      added,
        "added_to_sub_rate": round(added / a["total"] * 100, 2) if a["total"] else 0,
        "leads": leads,
    }


def fetch_email_data():
    """Full Smartlead fetch — same logic as _do_email_fetch in unified_dashboard."""
    log.info("Fetching Smartlead campaigns…")
    all_cmp    = get_all_campaigns()
    parent_map = {c["id"]: c for c in all_cmp}
    p_to_subs  = defaultdict(list)
    for c in all_cmp:
        if c.get("parent_campaign_id"):
            p_to_subs[c["parent_campaign_id"]].append(c)
    parents_with_subs = {pid: parent_map[pid] for pid in p_to_subs if pid in parent_map}

    # ── PHASE 1: subsequences (parallel) ────────────────────────
    subs = [c for c in all_cmp if c.get("parent_campaign_id")]
    log.info("Fetching %d subsequence stats with %d workers…", len(subs), FETCH_WORKERS)
    sub_emails  = defaultdict(set)
    sub_data    = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {ex.submit(_fetch_sub, s, parent_map): s for s in subs}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                r = fut.result()
                sub_data.append(r["row"])
                sub_emails[r["pid"]].update(r["emails"])
            except Exception as e:
                log.warning("sub fetch err: %s", e)
            if i % 50 == 0:
                log.info("  [phase1] %d/%d", i, len(subs))
    log.info("Phase 1 done in %.0fs (%d subs)", time.time() - t0, len(sub_data))

    # ── PHASE 2: parent campaigns (parallel) ─────────────────────
    parents = list(parents_with_subs.items())
    log.info("Fetching %d parent campaign stats with %d workers…", len(parents), FETCH_WORKERS)
    parent_data = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        futs = {ex.submit(_fetch_parent, pid, parent, sub_emails): pid
                for pid, parent in parents}
        for i, fut in enumerate(as_completed(futs), 1):
            try:
                parent_data.append(fut.result())
            except Exception as e:
                log.warning("parent fetch err: %s", e)
            if i % 25 == 0:
                log.info("  [phase2] %d/%d", i, len(parents))
    log.info("Phase 2 done in %.0fs (%d parents)", time.time() - t0, len(parent_data))

    return {"parent_analytics": parent_data, "sub_analytics": sub_data}


def fetch_linkedin_data():
    if not AIMFOX_AVAILABLE or not os.getenv("AIMFOX_API_KEY"):
        log.info("Skipping LinkedIn (no Aimfox credentials)")
        return None
    try:
        log.info("Fetching AimFox LinkedIn data…")
        client = AimfoxClient()
        accounts  = client.list_accounts()
        campaigns = client.list_campaigns()
        recent    = client.get_recent_leads()
        convos    = []
        for conv in client.list_conversations():
            owner, urn = conv.get("owner"), conv.get("conversation_urn")
            try:
                conv["_messages"] = (client.get_conversation_messages(owner, urn)
                                     if owner and urn else [])
            except Exception:
                conv["_messages"] = []
            convos.append(conv)
        log.info("LinkedIn: %d accounts, %d campaigns, %d conversations",
                 len(accounts), len(campaigns), len(convos))
        return {
            "accounts": accounts, "campaigns": campaigns,
            "recent_leads": recent, "conversations": convos,
        }
    except Exception as e:
        log.warning("LinkedIn fetch failed: %s", e)
        return None


def main():
    if not os.getenv("SMARTLEAD_API_KEY"):
        log.error("SMARTLEAD_API_KEY not set")
        sys.exit(1)

    started = datetime.utcnow().isoformat() + "Z"
    email_data    = fetch_email_data()
    linkedin_data = fetch_linkedin_data()
    finished      = datetime.utcnow().isoformat() + "Z"

    # Slim every campaign's leads (cap body chars + messages per lead)
    log.info("Slimming lead data…")
    for p in email_data["parent_analytics"]:
        slim_campaign(p)
    for s in email_data["sub_analytics"]:
        slim_campaign(s)

    # Slim AimFox conversation messages too — they can be huge
    if linkedin_data and "conversations" in linkedin_data:
        for cv in linkedin_data["conversations"]:
            msgs = cv.get("_messages") or []
            cv["_messages"] = [
                {
                    "body": (m.get("body") or "")[:MAX_MSG_BODY_CHARS],
                    "automated": m.get("automated", False),
                    "date": str(m.get("created_at", m.get("date", "")))[:19],
                    "sender": (m.get("sender") or {}).get("full_name", "") if isinstance(m.get("sender"), dict) else "",
                }
                for m in msgs[:MAX_MSGS_PER_LEAD]
            ]

    sent_total = sum(p.get("total", 0) for p in email_data["parent_analytics"])

    payload = {
        "version":     2,
        "started_at":  started,
        "finished_at": finished,
        "email":       email_data,
        "linkedin":    linkedin_data,
        "stats": {
            "parents":  len(email_data["parent_analytics"]),
            "subs":     len(email_data["sub_analytics"]),
            "total_sent": sent_total,
            "li_campaigns":     len((linkedin_data or {}).get("campaigns", [])),
            "li_conversations": len((linkedin_data or {}).get("conversations", [])),
        },
    }

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    raw_bytes = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
    with open(OUTPUT_PATH, "wb") as f:
        f.write(raw_bytes)
    with gzip.open(OUTPUT_GZ_PATH, "wb", compresslevel=9) as f:
        f.write(raw_bytes)

    raw_mb = len(raw_bytes) / (1024 * 1024)
    gz_mb  = os.path.getsize(OUTPUT_GZ_PATH) / (1024 * 1024)
    log.info("Wrote %s — %d parents, %d subs, %d sent  (raw %.1fMB → gzip %.1fMB)",
             OUTPUT_GZ_PATH, payload["stats"]["parents"],
             payload["stats"]["subs"], sent_total, raw_mb, gz_mb)


if __name__ == "__main__":
    main()
