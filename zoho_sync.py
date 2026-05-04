"""
Zoho CRM Sync for Aimfox
-------------------------
Pushes accepted LinkedIn connections and replies from Aimfox into Zoho CRM
as Contact records, then attaches conversation threads as Notes.

Usage:
    python zoho_sync.py                   # sync leads + conversations
    python zoho_sync.py --no-conversations # leads only (faster)
    python zoho_sync.py --module Leads    # push to Leads instead of Contacts

Zoho setup required (one-time):
    1. Create a custom field on Contacts (and/or Leads) module in Zoho CRM:
       Label: "LinkedIn URN"  |  API Name: LinkedIn_URN  |  Type: Single Line
    2. This field is used as the deduplication key so the same person is
       never created twice even if synced multiple times.
    3. If this field doesn't exist, the sync will fall back to name-based
       dedup (less reliable).
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))

from aimfox_client import AimfoxClient
from analytics import (
    fetch_accounts,
    fetch_conversations_with_messages,
    fetch_recent_leads,
)
from zoho_client import ZohoClient

log = logging.getLogger(__name__)

ZOHO_DEDUP_FIELD = "LinkedIn_URN"
BATCH_SIZE = 100

# Maps LinkedIn account name → Zoho Company field
ACCOUNT_TO_COMPANY = {
    "Ebru AVCI": "Feaam",
    "Prof. Dr.-Ing. Dieter Gerling": "Feaam",
    "Martin Kingsley": "K&M Property",
    "Garfield S. Campbell": "Stanford G",
}

# For Sanjay's account, determine company from campaign name
SANJAY_ACCOUNT = "Sanjay Swarup"
CAMPAIGN_TO_COMPANY = {
    "nexus": "Nexus",
    "kendra": "Kendra",
}


def _split_name(full_name: str) -> tuple[str, str]:
    parts = (full_name or "").strip().split(" ", 1)
    return parts[0], parts[1] if len(parts) > 1 else "-"


def fetch_lead_details(client: AimfoxClient, recent_leads: list) -> tuple[dict, dict]:
    """
    Returns ({urn: linkedin_url}, {urn: first_outreach_message}).
    Fetches lead profile URLs and outreach messages from conversations.
    """
    urn_to_url = {}
    urn_to_msg = {}
    lead_urns = {(e.get("target") or {}).get("urn") for e in recent_leads if (e.get("target") or {}).get("urn")}

    # LinkedIn URLs from lead details
    for event in recent_leads:
        target = event.get("target") or {}
        urn = target.get("urn", "")
        target_id = event.get("target_id")
        if not urn or not target_id or urn in urn_to_url:
            continue
        try:
            detail = client.get_lead(str(target_id))
            public_id = detail.get("public_identifier", "")
            if public_id:
                urn_to_url[urn] = f"https://www.linkedin.com/in/{public_id}/"
        except Exception as e:
            log.debug("Lead detail fetch failed for %s: %s", target_id, e)

    # Outreach messages from conversations
    try:
        convos = client.list_conversations()
        for conv in convos:
            participants = conv.get("participants", [])
            urn = (participants[0].get("urn") if participants else "") or ""
            if not urn or urn not in lead_urns or urn in urn_to_msg:
                continue
            owner = conv.get("owner", "")
            conv_urn = conv.get("conversation_urn", "")
            if not owner or not conv_urn:
                continue
            try:
                messages = client.get_conversation_messages(owner, conv_urn)
                for msg in messages:
                    body = (msg.get("body") or "").strip()
                    if msg.get("automated") and body:
                        urn_to_msg[urn] = body[:500]
                        break
            except Exception:
                pass
    except Exception as e:
        log.warning("Conversation fetch for messages failed: %s", e)

    return urn_to_url, urn_to_msg


def _build_contact_record(
    event: dict, acc_map: dict,
    urn_to_url: dict = None, urn_to_msg: dict = None
) -> tuple[str, dict]:
    """Return (linkedin_urn, zoho_record) for a single lead event."""
    target = event.get("target") or {}
    urn = (
        target.get("urn")
        or target.get("profile_urn")
        or target.get("linkedin_urn")
        or ""
    )
    first, last = _split_name(target.get("full_name", ""))
    campaign = event.get("campaign_name") or "-"
    account_name = acc_map.get(event.get("account_id", ""), "")
    transition = (event.get("transition") or "").lower()
    event_date = (event.get("timestamp") or "")[:10] or None

    company = ACCOUNT_TO_COMPANY.get(account_name, "")
    if not company and account_name == SANJAY_ACCOUNT:
        for keyword, co in CAMPAIGN_TO_COMPANY.items():
            if keyword in campaign.lower():
                company = co
                break

    connection_status = "Replied" if transition == "reply" else "Accepted"
    linkedin_url = (urn_to_url or {}).get(urn, "")
    outreach_msg = (urn_to_msg or {}).get(urn, "")
    description = outreach_msg if outreach_msg else f"Aimfox campaign: {campaign}"

    record = {
        "First_Name":               first,
        "Last_Name":                last,
        "Title":                    (target.get("occupation") or "")[:100],
        "Company":                  company,
        "Lead_Source":              "LinkedIn",
        "Description":              description,
        ZOHO_DEDUP_FIELD:           urn,
        "Aimfox_Connection_Status": connection_status,
        "AimFox_Campaign":          campaign,
        "Aimfox_Lead_ID":           str(event.get("target_id", "")),
        "LinkedIn_Url":             linkedin_url,
        "Accepted_date":            event_date,
        "AimFox_Replied":           1 if transition == "reply" else None,
    }

    record = {k: v for k, v in record.items() if v not in ("", None)}
    return urn, record


def leads_to_zoho_records(
    recent_leads: list, accounts: list,
    urn_to_url: dict = None, urn_to_msg: dict = None
) -> list[dict]:
    """Deduplicate by LinkedIn URN, keeping reply > accepted per person."""
    acc_map = {a["id"]: a.get("full_name", "") for a in accounts}
    seen: dict[str, dict] = {}

    for event in recent_leads:
        urn, record = _build_contact_record(event, acc_map, urn_to_url, urn_to_msg)
        if not urn:
            continue
        existing = seen.get(urn)
        if existing is None:
            seen[urn] = record
        elif (
            record.get("Aimfox_Connection_Status") == "Replied"
            and existing.get("Aimfox_Connection_Status") != "Replied"
        ):
            seen[urn] = record

    return list(seen.values())


def _upsert_batch(zoho: ZohoClient, records: list, module: str) -> dict:
    """Try upsert with LinkedIn_URN dedup field; fall back without it."""
    upsert_fn = zoho.upsert_contacts if module == "Contacts" else zoho.upsert_leads
    try:
        return upsert_fn(records, duplicate_check_fields=[ZOHO_DEDUP_FIELD])
    except Exception as e:
        log.warning(
            "Upsert with %s dedup failed (%s). "
            "Create a 'LinkedIn URN' custom field in Zoho to enable dedup. "
            "Retrying without dedup field...",
            ZOHO_DEDUP_FIELD, e,
        )
        return upsert_fn(records)


def upsert_all(zoho: ZohoClient, records: list, module: str) -> dict[str, int]:
    """Push records in batches of 100. Returns {'created': N, 'updated': N}."""
    created = updated = errors = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i: i + BATCH_SIZE]
        try:
            result = _upsert_batch(zoho, batch, module)
        except Exception as e:
            log.error("Batch %d upsert failed: %s", i // BATCH_SIZE + 1, e)
            errors += len(batch)
            continue

        for entry in result.get("data", []):
            if entry.get("code") == "SUCCESS":
                if entry.get("action") == "insert":
                    created += 1
                else:
                    updated += 1
            else:
                log.warning("Zoho record error: %s", entry)
                errors += 1

    if errors:
        log.warning("%d records failed to sync.", errors)
    return {"created": created, "updated": updated}


def build_urn_to_zoho_id(result_data: list, records: list) -> dict[str, str]:
    """Map LinkedIn URN → Zoho record ID from upsert response."""
    mapping = {}
    for i, entry in enumerate(result_data):
        if entry.get("code") == "SUCCESS" and i < len(records):
            zoho_id = entry.get("details", {}).get("id")
            urn = records[i].get(ZOHO_DEDUP_FIELD, "")
            if zoho_id and urn:
                mapping[urn] = zoho_id
    return mapping


def sync_conversations_as_notes(
    zoho: ZohoClient, conversations: list, urn_to_id: dict, module: str
):
    today = datetime.now().strftime("%Y-%m-%d")
    synced = 0

    for conv in conversations:
        participants = conv.get("participants", [])
        if not participants:
            continue

        urn = (
            participants[0].get("urn")
            or participants[0].get("profile_urn")
            or participants[0].get("linkedin_urn")
            or ""
        )
        zoho_id = urn_to_id.get(urn)
        if not zoho_id:
            continue

        messages = conv.get("_messages", [])
        if not messages:
            continue

        thread_lines = []
        for msg in messages[-15:]:
            body = (msg.get("body") or "").strip()[:500]
            if not body:
                continue
            sender = (msg.get("sender") or {}).get("full_name", "?")
            ts = str(msg.get("created_at", ""))[:10]
            auto = " [auto]" if msg.get("automated") else ""
            thread_lines.append(f"[{ts}] {sender}{auto}: {body}")

        if not thread_lines:
            continue

        try:
            zoho.add_note(
                parent_id=zoho_id,
                module=module,
                title=f"LinkedIn Conversation ({today})",
                content="\n".join(thread_lines),
            )
            synced += 1
        except Exception as e:
            log.warning("Failed to add note for URN %s: %s", urn, e)

    log.info("Conversation notes synced: %d", synced)


def run_sync(sync_conversations: bool = True, module: str = "Contacts") -> dict:
    log.info("=" * 50)
    log.info("Aimfox → Zoho CRM sync starting (module: %s)", module)

    aimfox = AimfoxClient()
    zoho = ZohoClient()

    log.info("Fetching Aimfox data...")
    accounts = fetch_accounts(aimfox)
    recent_leads = fetch_recent_leads(aimfox)

    log.info("Fetching LinkedIn URLs and outreach messages...")
    urn_to_url, urn_to_msg = fetch_lead_details(aimfox, recent_leads)
    log.info("Got %d LinkedIn URLs, %d outreach messages", len(urn_to_url), len(urn_to_msg))

    records = leads_to_zoho_records(recent_leads, accounts, urn_to_url, urn_to_msg)
    if not records:
        log.info("No leads to sync — nothing to do.")
        return {"created": 0, "updated": 0}

    log.info("Upserting %d %s records to Zoho CRM...", len(records), module)
    counts = upsert_all(zoho, records, module)
    log.info("Zoho sync: %d created, %d updated", counts["created"], counts["updated"])

    if sync_conversations:
        log.info("Fetching conversations for note sync...")
        try:
            conversations = fetch_conversations_with_messages(aimfox)
        except Exception as e:
            log.warning("Conversation fetch timed out or failed (%s) — skipping notes.", e)
            conversations = []

        if conversations:
            try:
                upsert_fn = zoho.upsert_contacts if module == "Contacts" else zoho.upsert_leads
                result = upsert_fn(records[:BATCH_SIZE], duplicate_check_fields=[ZOHO_DEDUP_FIELD])
                urn_to_id = build_urn_to_zoho_id(result.get("data", []), records[:BATCH_SIZE])
                sync_conversations_as_notes(zoho, conversations, urn_to_id, module)
            except Exception as e:
                log.warning("Note sync skipped: %s", e)

    # Build per-company breakdown and lead list
    company_counts = {}
    lead_list = []
    for rec in records:
        company = rec.get("Company") or "Unknown"
        company_counts[company] = company_counts.get(company, 0) + 1
        lead_list.append({
            "first_name":   rec.get("First_Name", ""),
            "last_name":    rec.get("Last_Name", ""),
            "company":      company,
            "title":        rec.get("Title", ""),
            "status":       rec.get("Aimfox_Connection_Status", ""),
            "campaign":     rec.get("AimFox_Campaign", ""),
            "linkedin_url": rec.get("LinkedIn_Url", ""),
            "linkedin_urn": rec.get(ZOHO_DEDUP_FIELD, ""),
        })

    stats = {
        "last_sync": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "created": counts["created"],
        "updated": counts["updated"],
        "total": counts["created"] + counts["updated"],
        "by_company": company_counts,
        "leads": lead_list,
    }

    stats_path = os.path.join(os.path.dirname(__file__), "reports", "zoho_sync_stats.json")
    os.makedirs(os.path.dirname(stats_path), exist_ok=True)
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    log.info("Zoho CRM sync complete.")
    return counts


def parse_args():
    p = argparse.ArgumentParser(description="Sync Aimfox leads to Zoho CRM")
    p.add_argument(
        "--no-conversations", action="store_true",
        help="Skip conversation note sync (faster)"
    )
    p.add_argument(
        "--module", choices=["Contacts", "Leads"], default="Leads",
        help="Zoho CRM module to push into (default: Leads)"
    )
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    args = parse_args()
    run_sync(
        sync_conversations=not args.no_conversations,
        module=args.module,
    )
