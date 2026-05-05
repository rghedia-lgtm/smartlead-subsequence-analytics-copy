"""One-shot: take whatever's in local cache.db, slim+gzip it, push to data-cache branch.

Bypasses GitHub Actions entirely. Use this to seed the cloud cache when the
Actions runner pool is jammed (free-tier capacity issue).
"""
import gzip
import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, "cache.db")
DATA_DIR = os.path.join(ROOT, "data")
GZ_PATH  = os.path.join(DATA_DIR, "cache.json.gz")

MAX_MSG_BODY_CHARS = 400
MAX_MSGS_PER_LEAD  = 8


def slim_lead(l):
    msgs = l.get("messages") or []
    lead_email = l.get("email", "")
    lead_name  = l.get("name") or lead_email or "Unknown"

    def _slim_msg(m):
        # Smartlead format: build_messages_from_history emits:
        #   {seq, subject, body, sent_time, open_time, click_time, is_reply}
        # AimFox format may use: {type/direction, from, time, body}
        # Normalise to the chat-thread renderer's contract.
        is_reply = m.get("is_reply")
        if is_reply is None:
            t = (m.get("type") or m.get("direction") or "").lower()
            is_reply = t in ("inbound", "received", "reply", "replied")
        return {
            "type": "inbound" if is_reply else "outbound",
            "from": (m.get("from") or
                     (lead_name if is_reply else "Us"))[:80],
            "subject": (m.get("subject") or "")[:200],
            "time": (m.get("time") or m.get("sent_time")
                     or m.get("received_at") or "")[:19],
            "body": (m.get("body") or "")[:MAX_MSG_BODY_CHARS],
        }

    return {
        "name":      lead_name,
        "email":     lead_email,
        "category":  l.get("category", ""),
        "company":   l.get("company", ""),
        "sent_time":  (l.get("sent_time")  or "")[:19],
        "open_time":  (l.get("open_time")  or "")[:19],
        "click_time": (l.get("click_time") or "")[:19],
        "reply_time": (l.get("reply_time") or "")[:19],
        "messages": [_slim_msg(m) for m in msgs[:MAX_MSGS_PER_LEAD]],
    }


def main():
    if not os.path.exists(DB_PATH):
        print(f"FATAL: no cache.db at {DB_PATH}")
        sys.exit(1)

    print(f"Reading {DB_PATH}…")
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT value, ts FROM kv WHERE key='email_cache'").fetchone()
    conn.close()
    if not row:
        print("FATAL: no email_cache row in DB")
        sys.exit(1)

    email_data = json.loads(row[0])
    email_ts   = row[1]

    parents = email_data.get("parent_analytics", [])
    subs    = email_data.get("sub_analytics", [])
    print(f"Loaded {len(parents)} parents, {len(subs)} subs (cache age {datetime.now().timestamp() - email_ts:.0f}s)")

    print("Slimming leads…")
    for p in parents:
        p["leads"] = [slim_lead(l) for l in (p.get("leads") or [])]
    for s in subs:
        s["leads"] = [slim_lead(l) for l in (s.get("leads") or [])]

    sent_total = sum(p.get("total", 0) for p in parents)

    # Fetch LinkedIn data live from AimFox (fast — already cached locally)
    linkedin_data = None
    try:
        sys.path.insert(0, ROOT)
        from aimfox_client import AimfoxClient
        from dotenv import load_dotenv
        load_dotenv(os.path.join(ROOT, ".env"))
        if os.getenv("AIMFOX_API_KEY"):
            print("Fetching LinkedIn data live from AimFox…")
            client    = AimfoxClient()
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
                # Cap each conv's messages
                conv["_messages"] = [
                    {
                        "body": (m.get("body") or "")[:MAX_MSG_BODY_CHARS],
                        "date": str(m.get("created_at", m.get("date", "")))[:19],
                        "automated": m.get("automated", False),
                        "sender": (m.get("sender") or {}).get("full_name", "") if isinstance(m.get("sender"), dict) else "",
                    }
                    for m in conv["_messages"][:MAX_MSGS_PER_LEAD]
                ]
                convos.append(conv)
            linkedin_data = {
                "accounts": accounts, "campaigns": campaigns,
                "recent_leads": recent, "conversations": convos,
            }
            print(f"  LinkedIn: {len(accounts)} accts, {len(campaigns)} camps, {len(convos)} convos")
    except Exception as e:
        print(f"  LinkedIn fetch failed: {e}")

    payload = {
        "version":     2,
        "started_at":  datetime.fromtimestamp(email_ts, tz=timezone.utc).isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "email":       {"parent_analytics": parents, "sub_analytics": subs},
        "linkedin":    linkedin_data,
        "stats": {
            "parents":  len(parents),
            "subs":     len(subs),
            "total_sent": sent_total,
            "li_campaigns":     len((linkedin_data or {}).get("campaigns", [])),
            "li_conversations": len((linkedin_data or {}).get("conversations", [])),
        },
        "source": "local-push",
    }

    os.makedirs(DATA_DIR, exist_ok=True)
    raw = json.dumps(payload, default=str, ensure_ascii=False).encode("utf-8")
    with gzip.open(GZ_PATH, "wb", compresslevel=9) as f:
        f.write(raw)
    raw_mb = len(raw) / 1024 / 1024
    gz_mb  = os.path.getsize(GZ_PATH) / 1024 / 1024
    print(f"Wrote {GZ_PATH}  raw {raw_mb:.1f}MB -> gzip {gz_mb:.1f}MB  ({sent_total:,} sent)")

    # Push to data-cache branch via git worktree
    tree_path = os.path.join(ROOT, "..", "_cache_tree")
    if os.path.exists(tree_path):
        print(f"Cleaning previous worktree {tree_path}")
        subprocess.run(["git", "worktree", "remove", "--force", tree_path],
                       cwd=ROOT, check=False)

    print("Setting up worktree on data-cache…")
    # Make sure we have the branch locally
    subprocess.run(["git", "fetch", "origin", "data-cache"], cwd=ROOT, check=False)
    r = subprocess.run(["git", "worktree", "add", tree_path, "data-cache"],
                       cwd=ROOT, capture_output=True, text=True)
    if r.returncode != 0:
        print("worktree add failed, trying as orphan branch:")
        print(r.stderr)
        subprocess.run(["git", "worktree", "add", "--detach", tree_path],
                       cwd=ROOT, check=True)
        subprocess.run(["git", "checkout", "--orphan", "data-cache"],
                       cwd=tree_path, check=True)
        # Wipe everything inherited from main
        for fn in os.listdir(tree_path):
            if fn != ".git":
                p = os.path.join(tree_path, fn)
                if os.path.isdir(p):
                    subprocess.run(["git", "rm", "-rf", fn], cwd=tree_path, check=False)
                else:
                    subprocess.run(["git", "rm", "-f", fn], cwd=tree_path, check=False)

    # Copy the gz file in
    target_dir = os.path.join(tree_path, "data")
    os.makedirs(target_dir, exist_ok=True)
    target = os.path.join(target_dir, "cache.json.gz")
    with open(GZ_PATH, "rb") as src, open(target, "wb") as dst:
        dst.write(src.read())

    # Remove old uncompressed cache.json if present
    old = os.path.join(target_dir, "cache.json")
    if os.path.exists(old):
        os.remove(old)
        subprocess.run(["git", "rm", "-f", "data/cache.json"], cwd=tree_path, check=False)

    subprocess.run(["git", "add", "data/cache.json.gz"], cwd=tree_path, check=True)
    diff = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=tree_path)
    if diff.returncode == 0:
        print("No changes to commit")
    else:
        msg = f"data: local push {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        subprocess.run(["git", "commit", "-m", msg], cwd=tree_path, check=True)
        push = subprocess.run(["git", "push", "origin", "HEAD:data-cache"],
                              cwd=tree_path, capture_output=True, text=True)
        if push.returncode != 0:
            print("PUSH FAILED:")
            print(push.stderr)
            sys.exit(1)
        print(f"OK Pushed to data-cache branch.\n{push.stderr}")

    # Clean up worktree
    subprocess.run(["git", "worktree", "remove", "--force", tree_path],
                   cwd=ROOT, check=False)


if __name__ == "__main__":
    main()
