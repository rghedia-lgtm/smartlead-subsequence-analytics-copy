"""Repush the existing data/cache.json.gz with aggressive lead slimming
so it fits comfortably in Render free tier's 512 MB RAM.

Drops leads with no engagement (only sent_time, no open/click/reply)
plus caps each campaign's lead list. Aggregate stats (total/opened/etc)
stay correct because they were already computed in the source data —
we're only trimming the LEAD detail array used for drill-down."""
import gzip
import json
import os
import subprocess
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
GZ_PATH = os.path.join(ROOT, "data", "cache.json.gz")

MAX_LEADS_PER_CAMPAIGN = 30  # aggressive cap so 14MB→<3MB; fits Render free


def slim_leads(leads):
    """Keep ENGAGED leads only (open/click/reply). Drop pure sent-only.
    Cap to MAX_LEADS_PER_CAMPAIGN. Keep messages for any lead that
    REPLIED (so the chat thread renders); strip them only on
    open-only / click-only leads where there was no conversation.
    Aggregate counts (total/opened/replied) are pre-computed and untouched."""
    if not leads:
        return leads
    POS = {"interested","meeting booked","positive","meeting request",
           "will buy","warm","demo request"}
    def priority(l):
        cat = (l.get("category") or "").lower()
        if cat in POS:        return 0
        if l.get("reply_time"): return 1
        if l.get("click_time"): return 2
        if l.get("open_time"):  return 3
        return 4
    engaged = sorted(
        [l for l in leads if l.get("open_time") or l.get("click_time") or l.get("reply_time")],
        key=priority,
    )[:MAX_LEADS_PER_CAMPAIGN]
    # Keep messages for leads that REPLIED (the conversation is the value).
    # For purely opened/clicked leads (no reply), strip messages — they're
    # just our outbound emails repeated and bloat the cache.
    for l in engaged:
        if not l.get("reply_time"):
            l["messages"] = []
    return engaged


def main():
    print(f"Loading {GZ_PATH}…")
    with gzip.open(GZ_PATH, "rb") as f:
        d = json.loads(f.read().decode("utf-8"))

    parents = d.get("email", {}).get("parent_analytics", [])
    subs    = d.get("email", {}).get("sub_analytics", [])

    p_lead_count_before = sum(len(p.get("leads") or []) for p in parents)
    s_lead_count_before = sum(len(s.get("leads") or []) for s in subs)

    for p in parents:
        p["leads"] = slim_leads(p.get("leads") or [])
    for s in subs:
        s["leads"] = slim_leads(s.get("leads") or [])

    p_lead_count_after = sum(len(p.get("leads") or []) for p in parents)
    s_lead_count_after = sum(len(s.get("leads") or []) for s in subs)

    print(f"Parent leads:   {p_lead_count_before:,} -> {p_lead_count_after:,}")
    print(f"Subseq leads:   {s_lead_count_before:,} -> {s_lead_count_after:,}")

    raw = json.dumps(d, default=str, ensure_ascii=False).encode("utf-8")
    with gzip.open(GZ_PATH, "wb", compresslevel=9) as f:
        f.write(raw)
    print(f"Wrote {GZ_PATH}: raw {len(raw)/1024/1024:.1f}MB -> gzip {os.path.getsize(GZ_PATH)/1024/1024:.1f}MB")

    # Push to main
    subprocess.run(["git", "add", "-f", "data/cache.json.gz"], cwd=ROOT, check=True)
    msg = f"data: slimmed cache to fit Render free tier ({datetime.now(timezone.utc).strftime('%H:%M UTC')})"
    r = subprocess.run(["git", "commit", "-m", msg], cwd=ROOT, capture_output=True, text=True)
    print(r.stdout)
    if r.returncode != 0 and "nothing to commit" not in r.stdout:
        print(r.stderr)
    push = subprocess.run(["git", "push", "origin", "main"], cwd=ROOT, capture_output=True, text=True)
    print(push.stdout)
    print(push.stderr)


if __name__ == "__main__":
    main()
