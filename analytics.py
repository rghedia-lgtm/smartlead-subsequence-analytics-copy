"""
Aimfox Analytics - fetch, aggregate, and display:
  - Campaign overview (state, targets, completion)
  - Per-campaign accepted connections & replies (from analytics/recent-leads)
  - Conversations with messages for accepted profiles
  - Account limits
"""

import sys
from collections import defaultdict
from datetime import datetime

from aimfox_client import AimfoxClient
from tabulate import tabulate
from colorama import Fore, Style, init

init(autoreset=True)

# Force UTF-8 output on Windows to avoid cp1252 encoding errors
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def _ts(ms):
    try:
        if isinstance(ms, (int, float)):
            return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d")
        return str(ms)[:10]
    except Exception:
        return str(ms)


def _s(v):
    """Replace None / empty with a dash."""
    return v if v is not None and v != "" else "-"


def header(text: str):
    print(f"\n{Fore.CYAN}{'='*64}")
    print(f"  {text}")
    print(f"{'='*64}{Style.RESET_ALL}")


# ── Accounts ──────────────────────────────────────────────────────────────────

def fetch_accounts(client: AimfoxClient) -> list:
    return client.list_accounts()


def print_accounts(accounts: list):
    header("LINKEDIN ACCOUNTS")
    rows = [
        [
            _s(a.get("full_name")),
            _s(a.get("id")),
            _s(a.get("state")),
            "YES" if a.get("premium") else "no",
            (_s(a.get("occupation")) or "")[:50],
        ]
        for a in accounts
    ]
    print(tabulate(rows, headers=["Name", "ID", "State", "Premium", "Occupation"], tablefmt="grid"))


# ── Campaigns ─────────────────────────────────────────────────────────────────

def fetch_campaigns(client: AimfoxClient) -> list:
    return client.list_campaigns()


def fetch_recent_leads(client: AimfoxClient) -> list:
    return client.get_recent_leads()


def build_campaign_stats(campaigns: list, recent_leads: list) -> list:
    accepted_per_campaign = defaultdict(int)
    replies_per_campaign = defaultdict(int)
    for lead in recent_leads:
        cid = lead.get("campaign_id")
        t = lead.get("transition", "")
        if t == "accepted":
            accepted_per_campaign[cid] += 1
        elif t == "reply":
            replies_per_campaign[cid] += 1

    rows = []
    for c in campaigns:
        cid = c.get("id")
        target = c.get("target_count") or 0
        completion = c.get("completion") or 0

        rows.append({
            "id": cid,
            "name": c.get("name", "-"),
            "state": c.get("state", "-"),
            "type": c.get("type", "-"),
            "outreach": c.get("outreach_type", "-"),
            "created": _ts(c.get("created_at")),
            "targets": target,
            "completion_pct": f"{completion * 100:.0f}%" if isinstance(completion, float) else f"{completion}%",
            "accepted_recent": accepted_per_campaign.get(cid, 0),
            "replies_recent": replies_per_campaign.get(cid, 0),
            "owners": ", ".join(c.get("owners") or []),
        })
    return rows


def print_campaigns(rows: list):
    header("CAMPAIGN OVERVIEW")
    table = [
        [
            r["name"][:45],
            r["state"],
            r["type"],
            r["targets"],
            r["completion_pct"],
            r["accepted_recent"],
            r["replies_recent"],
            r["created"],
        ]
        for r in rows
    ]
    print(tabulate(
        table,
        headers=["Campaign", "State", "Type", "Targets", "Done%", "Accepted*", "Replies*", "Created"],
        tablefmt="grid",
    ))
    print(f"  {Fore.YELLOW}* Accepted/Replies = events from recent analytics window{Style.RESET_ALL}")


# ── Recent lead events ────────────────────────────────────────────────────────

def print_recent_leads(recent_leads: list, accounts: list):
    header("RECENT LEAD EVENTS  (accepted connections & replies)")
    acc_map = {a["id"]: a.get("full_name", "?") for a in accounts}

    rows = []
    for e in recent_leads:
        target = e.get("target") or {}
        rows.append([
            e.get("transition", "-").upper(),
            target.get("full_name", "-"),
            (target.get("occupation") or "-")[:45],
            (e.get("campaign_name") or "-")[:35],
            acc_map.get(e.get("account_id", ""), e.get("account_id", "-")),
            (e.get("timestamp") or "-")[:10],
        ])
    print(tabulate(
        rows,
        headers=["Event", "Lead Name", "Occupation", "Campaign", "Account", "Date"],
        tablefmt="grid",
    ))


# ── Conversations & messages ──────────────────────────────────────────────────

def fetch_conversations_with_messages(client: AimfoxClient) -> list:
    conversations = client.list_conversations()
    enriched = []
    for conv in conversations:
        owner = conv.get("owner")
        urn = conv.get("conversation_urn")
        if owner and urn:
            try:
                messages = client.get_conversation_messages(owner, urn)
            except Exception:
                messages = []
            conv["_messages"] = messages
        else:
            conv["_messages"] = []
        enriched.append(conv)
    return enriched


def print_conversations(conversations: list):
    header("CONVERSATIONS WITH ACCEPTED PROFILES")
    for conv in conversations:
        participants = conv.get("participants", [])
        lead_name = participants[0].get("full_name", "-") if participants else "-"
        occupation = (participants[0].get("occupation") or "-")[:50] if participants else "-"
        messages = conv.get("_messages", [])
        connected = conv.get("connected", False)
        owner_id = conv.get("owner", "-")

        status_color = Fore.GREEN if connected else Fore.YELLOW
        print(f"\n  {status_color}[{'CONNECTED' if connected else 'NOT CONNECTED'}] {lead_name}{Style.RESET_ALL}  |  {occupation}")
        print(f"    Owner account: {owner_id}  |  Messages: {len(messages)}  |  Unread: {conv.get('unread_count', 0)}")

        if messages:
            print(f"    {Fore.WHITE}--- Thread ({len(messages)} messages) ---{Style.RESET_ALL}")
            for msg in messages:
                body = (msg.get("body") or "").strip()
                sender = (msg.get("sender") or {}).get("full_name", "unknown")
                ts = _ts(msg.get("created_at", ""))
                auto_tag = " [auto]" if msg.get("automated") else ""
                if body:
                    safe_body = body[:120].encode('ascii', errors='replace').decode('ascii')
                    print(f"    [{ts}] {Fore.CYAN}{sender}{auto_tag}{Style.RESET_ALL}: {safe_body}")
                elif msg.get("attachments"):
                    print(f"    [{ts}] {Fore.CYAN}{sender}{auto_tag}{Style.RESET_ALL}: [attachment]")


# ── Account limits ────────────────────────────────────────────────────────────

def print_account_limits(client: AimfoxClient, accounts: list):
    header("ACCOUNT WEEKLY LIMITS")
    rows = []
    for acc in accounts:
        try:
            lim = client.get_account_limits(acc["id"])
            rows.append([
                acc.get("full_name", "-"),
                acc["id"],
                lim.get("connect", "-"),
                lim.get("message_request", "-"),
                lim.get("inmail", "-"),
                acc.get("state", "-"),
            ])
        except Exception:
            rows.append([acc.get("full_name", "-"), acc["id"], "-", "-", "-", acc.get("state", "-")])
    print(tabulate(rows, headers=["Account", "ID", "Connect/wk", "Msg Req/wk", "InMail/wk", "State"], tablefmt="grid"))


# ── Global summary ────────────────────────────────────────────────────────────

def print_global_summary(campaigns: list, recent_leads: list, conversations: list, accounts: list):
    header("GLOBAL SUMMARY")
    total_targets = sum(c.get("targets", 0) or 0 for c in campaigns)
    total_accepted = sum(c.get("accepted_recent", 0) for c in campaigns)
    total_replies = sum(c.get("replies_recent", 0) for c in campaigns)
    active = sum(1 for c in campaigns if c.get("state") == "ACTIVE")
    done = sum(1 for c in campaigns if c.get("state") == "DONE")
    init_state = sum(1 for c in campaigns if c.get("state") == "INIT")

    rows = [
        ["Total Campaigns", len(campaigns)],
        ["  Active", active],
        ["  Done", done],
        ["  Init/Paused", init_state],
        ["Total Profiles Targeted", total_targets],
        ["Connections Accepted (recent)", total_accepted],
        ["Replies Received (recent)", total_replies],
        ["Active Conversations", len(conversations)],
        ["LinkedIn Accounts", len(accounts)],
    ]
    print(tabulate(rows, headers=["Metric", "Value"], tablefmt="grid"))
    print(f"\n  {Fore.YELLOW}Note: 'recent' = events in the Aimfox analytics window (last 7-30 days){Style.RESET_ALL}")
