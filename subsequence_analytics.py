import requests
import os
import csv
import json
import re
import logging
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SMARTLEAD_API_KEY")
BASE_URL = "https://server.smartlead.ai/api/v1"

RUN_TIME = datetime.now().strftime('%Y-%m-%d_%H-%M')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger()

# Client name mapping (keyword in campaign name → display name)
CLIENT_MAP = [
    ("FEAAM",       "FEAAM"),
    ("Wastestream", "Wastestream"),
    ("KM",          "KM"),
    ("Stanford",    "Stanford G"),
    ("Nexus",       "Nexus"),
    ("Henig",       "Henig"),
    ("USA",         "Kendra"),
]

# Lead categories that count as "positive"
POSITIVE_CATEGORIES = {
    "interested", "meeting booked", "positive", "meeting request",
    "will buy", "warm", "demo request",
}


def resolve_client(campaign_name):
    name_upper = campaign_name.upper()
    for keyword, client in CLIENT_MAP:
        if keyword.upper() in name_upper:
            return client
    return campaign_name


def get_all_campaigns():
    response = requests.get(f"{BASE_URL}/campaigns?api_key={API_KEY}")
    response.raise_for_status()
    return response.json()


def get_campaign_stats(campaign_id):
    all_stats = []
    offset = 0
    limit = 100
    while True:
        url = (
            f"{BASE_URL}/campaigns/{campaign_id}/statistics"
            f"?api_key={API_KEY}&limit={limit}&offset={offset}"
        )
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                log.warning(f"    Rate limited on campaign {campaign_id}, skipping.")
                break
            if not resp.content:
                log.warning(f"    Empty response for campaign {campaign_id} at offset {offset}, stopping.")
                break
            data = resp.json()
        except Exception as e:
            log.warning(f"    Failed to fetch stats for campaign {campaign_id} at offset {offset}: {e}")
            break
        all_stats.extend(data.get("data", []))
        total = int(data.get("total_stats", 0))
        offset += limit
        if offset >= total:
            break
    return all_stats


# Global cache so same email isn't looked up twice across campaigns
_lead_id_cache = {}


def get_lead_id(email):
    """Get numeric Smartlead lead_id for an email address."""
    if email in _lead_id_cache:
        return _lead_id_cache[email]
    try:
        resp = requests.get(f"{BASE_URL}/leads/?api_key={API_KEY}&email={email}", timeout=15)
        if resp.status_code == 200 and resp.content:
            data = resp.json()
            if isinstance(data, list) and data:
                lead_id = data[0].get('id')
            else:
                lead_id = data.get('id')
            _lead_id_cache[email] = lead_id
            return lead_id
    except Exception as e:
        log.warning(f"    Could not get lead_id for {email}: {e}")
    _lead_id_cache[email] = None
    return None


def get_lead_message_history(campaign_id, lead_id):
    """Fetch full sent+received message history for a lead in a campaign."""
    try:
        url = (
            f"{BASE_URL}/campaigns/{campaign_id}/leads/{lead_id}/message-history"
            f"?api_key={API_KEY}&show_plain_text_response=true"
        )
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200 and resp.content:
            data = resp.json()
            history = data.get('history') or data.get('list') or data.get('messages') or data.get('data') or []
            if not history and isinstance(data, list):
                history = data
            return history
    except Exception as e:
        log.warning(f"    Could not get message history for lead {lead_id}: {e}")
    return []


def build_messages_from_history(history):
    """Convert raw message-history list into normalised message dicts."""
    messages = []
    for item in history:
        # Direction: outbound = we sent it, inbound = reply from lead
        direction = (item.get('type') or item.get('direction') or 'outbound').lower()
        is_reply  = direction in ('inbound', 'received', 'reply')

        subject   = item.get('subject', '') or ''
        body_html = item.get('email_body') or item.get('message') or item.get('body') or ''
        body      = strip_html(body_html)[:3000]
        sent_time = (item.get('time') or item.get('sent_at') or
                     item.get('received_at') or item.get('created_at') or '')
        seq       = item.get('seq_number') or item.get('sequence_number') or 0

        # Engagement on outbound messages
        stats     = item.get('stats') or {}
        open_time = stats.get('open_time') or item.get('open_time') or ''
        click_time= stats.get('click_time') or item.get('click_time') or ''

        messages.append({
            'seq':       seq,
            'subject':   subject,
            'body':      body,
            'sent_time': sent_time,
            'open_time': open_time,
            'click_time':click_time,
            'is_reply':  is_reply,   # True = inbound reply from lead
        })

    messages.sort(key=lambda x: (x.get('sent_time', ''), x.get('seq', 0)))
    return messages


def enrich_replied_leads(campaign_id, leads):
    """
    For leads that have a reply_time, fetch full message history (incl. client reply).
    Only these leads get the enriched thread; others keep the stats-based messages.
    """
    replied = [l for l in leads if l.get('reply_time')]
    if not replied:
        return leads

    log.info(f"    Enriching {len(replied)} replied lead(s) with message history...")
    for lead in replied:
        lead_id = get_lead_id(lead['email'])
        if not lead_id:
            continue
        history = get_lead_message_history(campaign_id, lead_id)
        if history:
            lead['messages'] = build_messages_from_history(history)
    return leads


def strip_html(text):
    if not text:
        return ""
    clean = re.sub(r'<[^>]+>', ' ', text)
    clean = re.sub(r'&nbsp;', ' ', clean)
    clean = re.sub(r'&amp;', '&', clean)
    clean = re.sub(r'&lt;', '<', clean)
    clean = re.sub(r'&gt;', '>', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def collect_leads_detail(stats):
    """Build per-lead data with full message history from raw stats."""
    leads = {}
    for s in stats:
        email = s.get('lead_email', '')
        if not email:
            continue
        if email not in leads:
            leads[email] = {
                'email':          email,
                'name':           s.get('lead_name', '') or '',
                'category':       s.get('lead_category', '') or '',
                'open_time':      s.get('open_time') or None,
                'click_time':     s.get('click_time') or None,
                'reply_time':     s.get('reply_time') or None,
                'sent_time':      s.get('sent_time') or None,
                'is_bounced':     bool(s.get('is_bounced')),
                'is_unsubscribed':bool(s.get('is_unsubscribed')),
                'messages':       []
            }
        else:
            for f in ['open_time', 'click_time', 'reply_time', 'sent_time']:
                if s.get(f) and not leads[email][f]:
                    leads[email][f] = s[f]
            # Update category if we get a non-empty one
            if s.get('lead_category') and not leads[email]['category']:
                leads[email]['category'] = s['lead_category']

        subject = s.get('email_subject', '') or ''
        body    = strip_html(s.get('email_message', '') or '')
        if subject or body:
            leads[email]['messages'].append({
                'seq':        s.get('sequence_number') or 0,
                'subject':    subject,
                'body':       body[:2000],
                'sent_time':  s.get('sent_time') or '',
                'open_time':  s.get('open_time') or '',
                'click_time': s.get('click_time') or '',
            })

    for lead in leads.values():
        lead['messages'].sort(key=lambda x: (x['seq'], x['sent_time']))

    return list(leads.values())


def compute_analytics(leads):
    total       = len(leads)
    opened      = sum(1 for l in leads if l.get("open_time"))
    clicked     = sum(1 for l in leads if l.get("click_time"))
    replied     = sum(1 for l in leads if l.get("reply_time"))
    bounced     = sum(1 for l in leads if l.get("is_bounced"))
    unsubscribed= sum(1 for l in leads if l.get("is_unsubscribed"))
    positive    = sum(1 for l in leads if (l.get("category") or "").lower() in POSITIVE_CATEGORIES)

    def rate(n): return round(n / total * 100, 2) if total else 0

    return {
        "total":          total,
        "opened":         opened,
        "clicked":        clicked,
        "replied":        replied,
        "bounced":        bounced,
        "unsubscribed":   unsubscribed,
        "positive":       positive,
        "open_rate":      rate(opened),
        "click_rate":     rate(clicked),
        "reply_rate":     rate(replied),
        "positive_rate":  rate(positive),
    }


def generate_dashboard(parent_analytics, dashboard_data, html_path):
    run_date   = datetime.now().strftime('%Y-%m-%d %H:%M')
    total_subs = len(dashboard_data)

    # Unique clients (combined from both datasets)
    clients = sorted(set(
        [r['client'] for r in parent_analytics] +
        [r['parent'] for r in dashboard_data]
    ))
    client_options = '<option value="ALL">All Clients</option>' + ''.join(
        f'<option value="{c}">{c}</option>' for c in clients
    )

    parent_json = json.dumps(parent_analytics, ensure_ascii=False, default=str)
    sub_json    = json.dumps(dashboard_data,   ensure_ascii=False, default=str)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Campaign Analytics Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f1f5f9; color: #1e293b; min-height: 100vh; }}
.header {{ background: #ffffff; padding: 20px 32px; border-bottom: 1px solid #e2e8f0; box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
.header h1 {{ font-size: 20px; font-weight: 700; color: #0f172a; }}
.header p  {{ font-size: 12px; color: #64748b; margin-top: 3px; }}
.filter-bar {{ background: #ffffff; border-bottom: 1px solid #e2e8f0; padding: 12px 32px; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
.filter-bar label {{ font-size: 12px; color: #64748b; margin-right: 4px; font-weight: 500; }}
.filter-bar select, .filter-bar input[type=date] {{ background: #f8fafc; border: 1px solid #cbd5e1; color: #1e293b; padding: 6px 10px; border-radius: 6px; font-size: 13px; outline: none; }}
.filter-bar select:focus, .filter-bar input:focus {{ border-color: #3b82f6; }}
.btn {{ background: #2563eb; color: #fff; border: none; padding: 7px 18px; border-radius: 6px; font-size: 13px; cursor: pointer; font-weight: 600; }}
.btn:hover {{ background: #1d4ed8; }}
.btn-reset {{ background: #e2e8f0; color: #475569; }}
.btn-reset:hover {{ background: #cbd5e1; }}
.container {{ padding: 24px 32px; max-width: 1500px; margin: 0 auto; }}

/* Section headers */
.section-title {{ font-size: 15px; font-weight: 700; color: #0f172a; margin-bottom: 14px; padding-bottom: 8px; border-bottom: 2px solid #e2e8f0; display: flex; align-items: center; gap: 8px; }}
.section-title span {{ font-size: 12px; font-weight: 400; color: #94a3b8; }}
.section {{ margin-bottom: 28px; }}

/* Summary cards */
.cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr)); gap: 12px; margin-bottom: 20px; }}
.card {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
.card .label {{ font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }}
.card .value {{ font-size: 24px; font-weight: 700; margin-top: 4px; }}
.card .sub   {{ font-size: 11px; color: #94a3b8; margin-top: 2px; }}
.blue {{ color: #2563eb; }} .green {{ color: #16a34a; }} .purple {{ color: #7c3aed; }}
.yellow {{ color: #d97706; }} .slate {{ color: #475569; }} .rose {{ color: #e11d48; }}
.emerald {{ color: #059669; }}

/* Charts */
.charts {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); gap: 16px; margin-bottom: 20px; }}
.chart-card {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
.chart-card h2 {{ font-size: 12px; font-weight: 600; color: #475569; margin-bottom: 12px; }}
.chart-wrap {{ position: relative; height: 240px; }}

/* Tables */
.table-card {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 10px; padding: 16px; overflow-x: auto; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
th {{ text-align: left; padding: 9px 11px; background: #f8fafc; color: #64748b; font-weight: 700; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; border-bottom: 2px solid #e2e8f0; white-space: nowrap; }}
td {{ padding: 9px 11px; border-bottom: 1px solid #f1f5f9; color: #334155; }}
tr:hover td {{ background: #f8fafc; }}
.clickable {{ cursor: pointer; color: #2563eb; font-weight: 600; text-decoration: underline dotted; }}
.clickable:hover {{ color: #1d4ed8; }}
.rate  {{ color: #94a3b8; font-size: 11px; }}
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 700; color: #fff; }}
.no-data {{ text-align: center; padding: 32px; color: #94a3b8; font-size: 14px; }}
.hint {{ font-size: 11px; color: #94a3b8; font-weight: 400; margin-left: 8px; }}

/* Funnel bar */
.funnel-row {{ display: flex; align-items: center; gap: 10px; margin-bottom: 8px; }}
.funnel-label {{ width: 110px; font-size: 12px; color: #64748b; text-align: right; flex-shrink: 0; }}
.funnel-bar-wrap {{ flex: 1; background: #f1f5f9; border-radius: 4px; height: 22px; overflow: hidden; }}
.funnel-bar {{ height: 100%; border-radius: 4px; display: flex; align-items: center; padding-left: 8px; font-size: 11px; font-weight: 700; color: #fff; transition: width 0.4s; }}
.funnel-val {{ width: 80px; font-size: 12px; color: #475569; flex-shrink: 0; }}

/* Modal */
.modal-overlay {{ display: none; position: fixed; inset: 0; background: rgba(15,23,42,0.5); z-index: 1000; align-items: center; justify-content: center; }}
.modal-overlay.open {{ display: flex; }}
.modal {{ background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; width: 92vw; max-width: 1100px; height: 82vh; display: flex; flex-direction: column; overflow: hidden; box-shadow: 0 20px 60px rgba(0,0,0,0.15); }}
.modal-header {{ padding: 14px 20px; border-bottom: 1px solid #e2e8f0; display: flex; justify-content: space-between; align-items: center; flex-shrink: 0; background: #f8fafc; }}
.modal-header h3 {{ font-size: 14px; font-weight: 700; color: #0f172a; }}
.modal-close {{ background: none; border: none; color: #94a3b8; font-size: 22px; cursor: pointer; }}
.modal-close:hover {{ color: #1e293b; }}
.modal-body {{ display: flex; flex: 1; overflow: hidden; }}
.lead-list {{ width: 290px; border-right: 1px solid #e2e8f0; overflow-y: auto; flex-shrink: 0; background: #f8fafc; }}
.lead-item {{ padding: 11px 14px; border-bottom: 1px solid #f1f5f9; cursor: pointer; }}
.lead-item:hover {{ background: #f1f5f9; }}
.lead-item.active {{ background: #eff6ff; border-left: 3px solid #2563eb; }}
.lead-name  {{ font-size: 13px; font-weight: 600; color: #1e293b; }}
.lead-email {{ font-size: 11px; color: #64748b; margin-top: 1px; word-break: break-all; }}
.lead-meta  {{ font-size: 10px; color: #94a3b8; margin-top: 4px; }}
.lead-cat   {{ display: inline-block; padding: 1px 6px; border-radius: 3px; font-size: 10px; font-weight: 700; background: #dcfce7; color: #16a34a; margin-top: 3px; }}
.msg-panel  {{ flex: 1; overflow-y: auto; padding: 18px; background: #ffffff; }}
.msg-panel .placeholder {{ color: #94a3b8; font-size: 13px; text-align: center; padding-top: 60px; }}
.msg-thread {{ display: flex; flex-direction: column; gap: 14px; }}
.msg-bubble {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 13px; }}
.msg-meta   {{ display: flex; justify-content: space-between; margin-bottom: 6px; flex-wrap: wrap; gap: 4px; }}
.msg-seq    {{ font-size: 11px; color: #2563eb; font-weight: 700; }}
.msg-date   {{ font-size: 11px; color: #94a3b8; }}
.msg-subject{{ font-size: 13px; font-weight: 600; color: #1e293b; margin-bottom: 7px; }}
.msg-body   {{ font-size: 12px; color: #475569; line-height: 1.6; white-space: pre-wrap; word-break: break-word; }}
.msg-tags   {{ display: flex; gap: 6px; margin-top: 8px; flex-wrap: wrap; }}
.tag        {{ display: inline-block; padding: 2px 7px; border-radius: 3px; font-size: 10px; font-weight: 700; }}
.tag-open   {{ background: #dcfce7; color: #16a34a; }}
.tag-click  {{ background: #dbeafe; color: #2563eb; }}
.tag-reply  {{ background: #fef3c7; color: #d97706; }}
.msg-bubble.inbound {{ background: #fffbeb; border-color: #fde68a; border-left: 3px solid #f59e0b; }}
.msg-bubble.inbound .msg-subject {{ color: #92400e; }}
.msg-bubble.inbound .msg-body {{ color: #78350f; }}
.msg-direction {{ font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 3px; margin-right: 6px; }}
.dir-out {{ background: #dbeafe; color: #1d4ed8; }}
.dir-in  {{ background: #fef3c7; color: #b45309; }}
.empty-list {{ padding: 20px; text-align: center; color: #94a3b8; font-size: 13px; }}
</style>
</head>
<body>

<div class="header">
  <h1>Campaign Analytics Dashboard</h1>
  <p>Generated: {run_date} &nbsp;|&nbsp; <span id="subCount">{total_subs}</span> subsequences across <span id="parentCount">{len(parent_analytics)}</span> campaigns</p>
</div>

<div class="filter-bar">
  <div><label>Client</label>
    <select id="clientFilter">{client_options}</select>
  </div>
  <div><label>From</label><input type="date" id="dateFrom"></div>
  <div><label>To</label><input type="date" id="dateTo"></div>
  <button class="btn" onclick="applyFilters()">Apply</button>
  <button class="btn btn-reset" onclick="resetFilters()">Reset</button>
  <span id="filterStatus" style="font-size:12px;color:#64748b;"></span>
</div>

<div class="container">

  <!-- ── COMBINED OVERVIEW ──────────────────────────────────── -->
  <div class="section">
    <div class="section-title">🔗 Combined Overview <span>— full pipeline across campaigns &amp; subsequences</span></div>

    <div class="cards" id="combinedCards"></div>

    <div class="charts" style="grid-template-columns:1fr;">
      <div class="chart-card"><h2>Full Pipeline by Client (Sent → Lead. Opened → Added to Sub → Sub Opened → Sub Replied)</h2><div class="chart-wrap" style="height:280px;"><canvas id="combinedPipelineChart"></canvas></div></div>
    </div>

    <div class="table-card">
      <div id="combinedTableContainer"></div>
    </div>
  </div>

  <!-- ── MAIN CAMPAIGN ANALYTICS ─────────────────────────────── -->
  <div class="section">
    <div class="section-title">📊 Main Campaign Analytics <span>— parent campaigns performance</span></div>

    <div class="cards" id="mainCards"></div>

    <div class="charts">
      <div class="chart-card"><h2>Opened Leads by Client</h2><div class="chart-wrap" style="height:260px;"><canvas id="mainFunnelChart"></canvas></div></div>
      <div class="chart-card"><h2>Added to Sub by Client</h2><div class="chart-wrap" style="height:260px;"><canvas id="mainSubChart"></canvas></div></div>
    </div>

    <div class="table-card">
      <div id="mainTableContainer"></div>
    </div>
  </div>

  <!-- ── SUBSEQUENCE ANALYTICS ──────────────────────────────── -->
  <div class="section">
    <div class="section-title">🔁 Subsequence Analytics <span class="hint">— double-click Opened or Clicked to see lead details</span></div>

    <div class="cards" id="subCards"></div>

    <div class="charts">
      <div class="chart-card"><h2>Open Rate (%) by Subsequence</h2><div class="chart-wrap"><canvas id="openChart"></canvas></div></div>
      <div class="chart-card"><h2>Click Rate (%) by Subsequence</h2><div class="chart-wrap"><canvas id="clickChart"></canvas></div></div>
      <div class="chart-card"><h2>Reply Rate (%) by Subsequence</h2><div class="chart-wrap"><canvas id="replyChart"></canvas></div></div>
      <div class="chart-card"><h2>Total Leads by Subsequence</h2><div class="chart-wrap"><canvas id="leadsChart"></canvas></div></div>
    </div>

    <div class="table-card">
      <div id="subTableContainer"></div>
    </div>
  </div>

</div>

<!-- Lead Detail Modal -->
<div class="modal-overlay" id="modalOverlay">
  <div class="modal">
    <div class="modal-header">
      <h3 id="modalTitle">Lead Details</h3>
      <button class="modal-close" onclick="closeModal()">×</button>
    </div>
    <div class="modal-body">
      <div class="lead-list" id="leadList"></div>
      <div class="msg-panel" id="msgPanel"><div class="placeholder">← Select a lead to view message history</div></div>
    </div>
  </div>
</div>

<script>
const PARENT_DATA = {parent_json};
const SUB_DATA    = {sub_json};

let filtParent = [];
let filtSub    = [];
let charts     = {{}};

// ── Filtering ────────────────────────────────────────────────────────────────
function applyFilters() {{
  const client = document.getElementById('clientFilter').value;
  const from   = document.getElementById('dateFrom').value;
  const to     = document.getElementById('dateTo').value;

  filtParent = PARENT_DATA.filter(r => client === 'ALL' || r.client === client);

  filtSub = SUB_DATA.map(row => {{
    if (client !== 'ALL' && row.parent !== client) return null;
    let leads = row.leads || [];
    if (from || to) {{
      leads = leads.filter(l => {{
        const d = (l.sent_time || l.open_time || '').substring(0, 10);
        if (!d) return false;
        if (from && d < from) return false;
        if (to   && d > to)   return false;
        return true;
      }});
    }}
    return {{ ...row, leads, ...recompute(leads) }};
  }}).filter(r => r !== null);

  const parts = [];
  if (client !== 'ALL') parts.push(client);
  if (from || to) parts.push((from||'…') + ' → ' + (to||'…'));
  document.getElementById('filterStatus').textContent = parts.length ? 'Filtered: ' + parts.join(', ') : '';

  render();
}}

function resetFilters() {{
  document.getElementById('clientFilter').value = 'ALL';
  document.getElementById('dateFrom').value = '';
  document.getElementById('dateTo').value = '';
  document.getElementById('filterStatus').textContent = '';
  filtParent = PARENT_DATA.slice();
  filtSub    = SUB_DATA.map(r => ({{ ...r, ...recompute(r.leads || []) }}));
  render();
}}

function recompute(leads) {{
  const total        = leads.length;
  const opened       = leads.filter(l => l.open_time).length;
  const clicked      = leads.filter(l => l.click_time).length;
  const replied      = leads.filter(l => l.reply_time).length;
  const bounced      = leads.filter(l => l.is_bounced).length;
  const unsubscribed = leads.filter(l => l.is_unsubscribed).length;
  const positive     = leads.filter(l => POSITIVE_CATS.has((l.category||'').toLowerCase())).length;
  const r = n => total ? +(n/total*100).toFixed(2) : 0;
  return {{ total, opened, clicked, replied, bounced, unsubscribed, positive,
            open_rate: r(opened), click_rate: r(clicked), reply_rate: r(replied), positive_rate: r(positive) }};
}}

const POSITIVE_CATS = new Set([
  "interested","meeting booked","positive","meeting request","will buy","warm","demo request"
]);

// ── Render ────────────────────────────────────────────────────────────────────
function render() {{
  renderCombinedCards();
  renderCombinedCharts();
  renderCombinedTable();
  renderMainCards();
  renderMainCharts();
  renderMainTable();
  renderSubCards();
  renderSubCharts();
  renderSubTable();
  document.getElementById('subCount').textContent    = filtSub.length;
  document.getElementById('parentCount').textContent = filtParent.length;
}}

// ── Combined Overview ─────────────────────────────────────────────────────────
function renderCombinedCards() {{
  const totSent   = filtParent.reduce((s,r) => s + r.total,        0);
  const totPOpen  = filtParent.reduce((s,r) => s + r.opened,       0);
  const totPReply = filtParent.reduce((s,r) => s + r.replied,      0);
  const totSub    = filtParent.reduce((s,r) => s + r.added_to_sub, 0);
  const totPos    = filtParent.reduce((s,r) => s + r.positive,     0);
  const totSOpen   = filtSub.reduce((s,r)   => s + r.opened,       0);
  const totSClick  = filtSub.reduce((s,r)   => s + r.clicked,      0);
  const totSReply  = filtSub.reduce((s,r)   => s + r.replied,      0);
  const totSLeads  = filtSub.reduce((s,r)   => s + r.total,        0);
  document.getElementById('combinedCards').innerHTML = `
    <div class="card"><div class="label">Total Sent</div><div class="value slate">${{totSent.toLocaleString()}}</div><div class="sub">${{filtParent.length}} campaigns</div></div>
    <div class="card"><div class="label">Lead. Opened</div><div class="value green">${{totPOpen.toLocaleString()}}</div><div class="sub">${{totSent ? (totPOpen/totSent*100).toFixed(1) : 0}}% of sent</div></div>
    <div class="card"><div class="label">Lead. Replied</div><div class="value purple">${{totPReply.toLocaleString()}}</div><div class="sub">${{totSent ? (totPReply/totSent*100).toFixed(1) : 0}}% of sent</div></div>
    <div class="card"><div class="label">Added to Sub</div><div class="value blue">${{totSub.toLocaleString()}}</div><div class="sub">${{totSent ? (totSub/totSent*100).toFixed(1) : 0}}% of sent</div></div>
    <div class="card"><div class="label">Sub Opened</div><div class="value emerald">${{totSOpen.toLocaleString()}}</div><div class="sub">${{totSLeads ? (totSOpen/totSLeads*100).toFixed(1) : 0}}% of sub leads</div></div>
    <div class="card"><div class="label">Sub Clicked</div><div class="value blue">${{totSClick.toLocaleString()}}</div><div class="sub">${{totSLeads ? (totSClick/totSLeads*100).toFixed(1) : 0}}% of sub leads</div></div>
    <div class="card"><div class="label">Sub Replied</div><div class="value yellow">${{totSReply.toLocaleString()}}</div><div class="sub">${{totSLeads ? (totSReply/totSLeads*100).toFixed(1) : 0}}% of sub leads</div></div>
    <div class="card"><div class="label">Total Positive</div><div class="value rose">${{totPos.toLocaleString()}}</div><div class="sub">${{totSent ? (totPos/totSent*100).toFixed(1) : 0}}% conversion</div></div>
  `;
}}

function renderCombinedCharts() {{
  const clientMap = {{}};
  filtParent.forEach(r => {{
    if (!clientMap[r.client]) clientMap[r.client] = {{sent:0, pOpened:0, pReplied:0, addedToSub:0, positive:0, sOpened:0, sReplied:0}};
    clientMap[r.client].sent       += r.total;
    clientMap[r.client].pOpened    += r.opened;
    clientMap[r.client].pReplied   += r.replied;
    clientMap[r.client].addedToSub += r.added_to_sub;
    clientMap[r.client].positive   += r.positive;
  }});
  filtSub.forEach(r => {{
    const c = r.parent;
    if (!clientMap[c]) clientMap[c] = {{sent:0, pOpened:0, pReplied:0, addedToSub:0, positive:0, sOpened:0, sReplied:0}};
    clientMap[c].sOpened  += r.opened;
    clientMap[c].sReplied += r.replied;
  }});
  const labels = Object.keys(clientMap);
  const data   = Object.values(clientMap);
  if (charts['combinedPipelineChart']) charts['combinedPipelineChart'].destroy();
  charts['combinedPipelineChart'] = new Chart(document.getElementById('combinedPipelineChart'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'Sent',         data: data.map(d => d.sent),       backgroundColor: '#94a3b8', borderRadius: 4 }},
        {{ label: 'Lead. Opened', data: data.map(d => d.pOpened),    backgroundColor: '#22d3ee', borderRadius: 4 }},
        {{ label: 'Lead. Replied',data: data.map(d => d.pReplied),   backgroundColor: '#c084fc', borderRadius: 4 }},
        {{ label: 'Added to Sub', data: data.map(d => d.addedToSub), backgroundColor: '#38bdf8', borderRadius: 4 }},
        {{ label: 'Sub Opened',   data: data.map(d => d.sOpened),    backgroundColor: '#4ade80', borderRadius: 4 }},
        {{ label: 'Sub Replied',  data: data.map(d => d.sReplied),   backgroundColor: '#fb923c', borderRadius: 4 }},
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ labels: {{ color: '#64748b', font: {{ size: 11 }} }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#64748b', font: {{ size: 11 }} }}, grid: {{ color: '#f8fafc' }} }},
        y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#f1f5f9' }} }}
      }}
    }}
  }});
}}

function renderCombinedTable() {{
  const clientMap = {{}};
  filtParent.forEach(r => {{
    if (!clientMap[r.client]) clientMap[r.client] = {{sent:0, pOpened:0, pReplied:0, positive:0, addedToSub:0, sLeads:0, sOpened:0, sClicked:0, sReplied:0}};
    clientMap[r.client].sent       += r.total;
    clientMap[r.client].pOpened    += r.opened;
    clientMap[r.client].pReplied   += r.replied;
    clientMap[r.client].positive   += r.positive;
    clientMap[r.client].addedToSub += r.added_to_sub;
  }});
  filtSub.forEach(r => {{
    const c = r.parent;
    if (!clientMap[c]) clientMap[c] = {{sent:0, pOpened:0, pReplied:0, positive:0, addedToSub:0, sLeads:0, sOpened:0, sClicked:0, sReplied:0}};
    clientMap[c].sLeads   += r.total;
    clientMap[c].sOpened  += r.opened;
    clientMap[c].sClicked += r.clicked;
    clientMap[c].sReplied += r.replied;
  }});
  const container = document.getElementById('combinedTableContainer');
  const rows = Object.entries(clientMap);
  if (!rows.length) {{ container.innerHTML = '<div class="no-data">No data.</div>'; return; }}
  window._combinedRowsData = rows;
  let html = `<table><thead><tr>
    <th>Client</th>
    <th>Sent</th><th>Lead. Opened</th><th>Lead. Replied</th><th>Added to Sub</th>
    <th>Sub Leads</th><th>Sub Opened</th><th>Sub Clicked</th><th>Sub Replied</th><th>Positive</th>
  </tr></thead><tbody>`;
  rows.forEach(([client, d], idx) => {{
    const pct = (n, tot) => tot ? (n/tot*100).toFixed(1) : 0;
    const mk = (val, tot, type, style) => {{
      const pctStr = pct(val, tot);
      const inner = `${{val}} <span class="rate">(${{pctStr}}%)</span>`;
      if (val > 0) return `<span class="clickable"${{style ? ' style="'+style+'"' : ''}} ondblclick="openModalCombined(${{idx}},'${{type}}')">${{inner}}</span>`;
      return style ? `<span style="${{style}}">${{inner}}</span>` : inner;
    }};
    html += `<tr>
      <td><strong>${{esc(client)}}</strong></td>
      <td>${{d.sent}}</td>
      <td>${{mk(d.pOpened,  d.sent,   'pOpened',  '')}}</td>
      <td>${{mk(d.pReplied, d.sent,   'pReplied', '')}}</td>
      <td>${{mk(d.addedToSub, d.sent, 'addedToSub', 'color:#2563eb')}}</td>
      <td>${{d.sLeads}}</td>
      <td>${{mk(d.sOpened,  d.sLeads, 'sOpened',  '')}}</td>
      <td>${{mk(d.sClicked, d.sLeads, 'sClicked', '')}}</td>
      <td>${{mk(d.sReplied, d.sLeads, 'sReplied', '')}}</td>
      <td>${{mk(d.positive, d.sent,   'positive', 'color:#059669')}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

function openModalCombined(rowIdx, type) {{
  const [client] = window._combinedRowsData[rowIdx];
  let leads = [];
  const typeLabels = {{
    pOpened: '📬 Lead. Opened', pReplied: '💬 Lead. Replied',
    positive: '✅ Positive', addedToSub: '➕ Added to Sub',
    sOpened: '📬 Sub Opened', sClicked: '🖱 Sub Clicked', sReplied: '💬 Sub Replied'
  }};
  if (type === 'pOpened' || type === 'pReplied' || type === 'positive') {{
    filtParent.filter(r => r.client === client).forEach(r => {{
      (r.leads || []).forEach(l => {{
        if (type === 'pOpened'  && l.open_time)  leads.push(l);
        if (type === 'pReplied' && l.reply_time) leads.push(l);
        if (type === 'positive' && POSITIVE_CATS.has((l.category||'').toLowerCase())) leads.push(l);
      }});
    }});
  }} else if (type === 'addedToSub') {{
    const seen = new Set();
    filtSub.filter(r => r.parent === client).forEach(r => {{
      (r.leads || []).forEach(l => {{
        if (!seen.has(l.email)) {{ seen.add(l.email); leads.push(l); }}
      }});
    }});
  }} else {{
    filtSub.filter(r => r.parent === client).forEach(r => {{
      (r.leads || []).forEach(l => {{
        if (type === 'sOpened'  && l.open_time)  leads.push(l);
        if (type === 'sClicked' && l.click_time) leads.push(l);
        if (type === 'sReplied' && l.reply_time) leads.push(l);
      }});
    }});
  }}
  document.getElementById('modalTitle').textContent = (typeLabels[type] || type) + ' — ' + client;
  window._modalLeads = leads;
  const listEl = document.getElementById('leadList');
  if (!leads.length) {{
    listEl.innerHTML = '<div class="empty-list">No lead details available.</div>';
    document.getElementById('msgPanel').innerHTML = '<div class="placeholder">No leads found.</div>';
  }} else {{
    listEl.innerHTML = leads.map((l, i) => `
      <div class="lead-item" id="li-${{i}}" onclick="showMessages(${{i}})">
        <div class="lead-name">${{esc(l.name || '(No Name)')}}</div>
        <div class="lead-email">${{esc(l.email)}}</div>
        <div class="lead-meta">
          ${{l.open_time  ? '📬 ' + fmtDate(l.open_time)  : ''}}
          ${{l.click_time ? ' 🖱 ' + fmtDate(l.click_time) : ''}}
          ${{l.reply_time ? ' 💬 ' + fmtDate(l.reply_time) : ''}}
        </div>
        ${{l.category ? '<div class="lead-cat">' + esc(l.category) + '</div>' : ''}}
      </div>`).join('');
    document.getElementById('msgPanel').innerHTML = '<div class="placeholder">← Select a lead</div>';
  }}
  document.getElementById('modalOverlay').classList.add('open');
}}

// ── Main Campaign ─────────────────────────────────────────────────────────────
function renderMainCards() {{
  const totSent  = filtParent.reduce((s,r) => s + r.total, 0);
  const totOpen  = filtParent.reduce((s,r) => s + r.opened, 0);
  const totPos   = filtParent.reduce((s,r) => s + r.positive, 0);
  const totSub   = filtParent.reduce((s,r) => s + r.added_to_sub, 0);
  const totReply = filtParent.reduce((s,r) => s + r.replied, 0);
  const avgOpen  = totSent ? (totOpen/totSent*100).toFixed(1) : 0;
  const avgPos   = totSent ? (totPos/totSent*100).toFixed(1)  : 0;
  const avgSub   = totSent ? (totSub/totSent*100).toFixed(1)  : 0;

  document.getElementById('mainCards').innerHTML = `
    <div class="card"><div class="label">Total Sent</div><div class="value slate">${{totSent.toLocaleString()}}</div><div class="sub">${{filtParent.length}} campaigns</div></div>
    <div class="card"><div class="label">Opened</div><div class="value green">${{totOpen.toLocaleString()}}</div><div class="sub">Avg ${{avgOpen}}%</div></div>
    <div class="card"><div class="label">Positive</div><div class="value emerald">${{totPos.toLocaleString()}}</div><div class="sub">Avg ${{avgPos}}%</div></div>
    <div class="card"><div class="label">Replied</div><div class="value purple">${{totReply.toLocaleString()}}</div><div class="sub">&nbsp;</div></div>
    <div class="card"><div class="label">Added to Sub</div><div class="value blue">${{totSub.toLocaleString()}}</div><div class="sub">Avg ${{avgSub}}%</div></div>
  `;
}}

function renderMainCharts() {{
  const PIE_COLORS = ['#f43f5e','#f97316','#eab308','#22c55e','#06b6d4','#3b82f6','#8b5cf6','#ec4899','#14b8a6','#a855f7','#ef4444','#84cc16','#0ea5e9'];
  const clientLabels = [...new Set(filtParent.map(r => r.client))];
  const clientOpened = clientLabels.map(c => filtParent.filter(r => r.client===c).reduce((s,r)=>s+r.opened,0));
  const clientSub    = clientLabels.map(c => filtParent.filter(r => r.client===c).reduce((s,r)=>s+r.added_to_sub,0));
  const pieOpts = (title) => ({{
    responsive: true, maintainAspectRatio: false,
    plugins: {{
      legend: {{ position: 'right', labels: {{ color: '#475569', font: {{ size: 11 }}, padding: 12, boxWidth: 14 }} }},
      tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.label}}: ${{ctx.parsed.toLocaleString()}} (${{(ctx.parsed/ctx.dataset.data.reduce((a,b)=>a+b,0)*100).toFixed(1)}}%)` }} }}
    }}
  }});

  if (charts['mainFunnelChart']) charts['mainFunnelChart'].destroy();
  charts['mainFunnelChart'] = new Chart(document.getElementById('mainFunnelChart'), {{
    type: 'doughnut',
    data: {{
      labels: clientLabels,
      datasets: [{{ data: clientOpened, backgroundColor: PIE_COLORS, borderWidth: 2, borderColor: '#fff', hoverOffset: 8 }}]
    }},
    options: pieOpts('Opened Leads by Client')
  }});

  if (charts['mainSubChart']) charts['mainSubChart'].destroy();
  charts['mainSubChart'] = new Chart(document.getElementById('mainSubChart'), {{
    type: 'doughnut',
    data: {{
      labels: clientLabels,
      datasets: [{{ data: clientSub, backgroundColor: PIE_COLORS, borderWidth: 2, borderColor: '#fff', hoverOffset: 8 }}]
    }},
    options: pieOpts('Added to Sub by Client')
  }});
}}

function renderMainTable() {{
  if (!filtParent.length) {{
    document.getElementById('mainTableContainer').innerHTML = '<div class="no-data">No data.</div>';
    return;
  }}
  let html = `<table><thead><tr>
    <th>Client</th><th>Campaign</th><th>Status</th>
    <th>Total Sent</th><th>Opened</th><th>Clicked</th><th>Replied</th>
    <th>Positive</th><th>Added to Sub</th><th>Bounced</th>
  </tr></thead><tbody>`;
  filtParent.forEach((r, i) => {{
    const sc = r.status === 'ACTIVE' ? '#22c55e' : '#94a3b8';
    const oCell = r.opened  > 0 ? `<span class="clickable" ondblclick="openModalParent(${{i}},'opened')">${{r.opened}} <span class="rate">(${{r.open_rate}}%)</span></span>`  : `${{r.opened}} <span class="rate">(${{r.open_rate}}%)</span>`;
    const cCell = r.clicked > 0 ? `<span class="clickable" ondblclick="openModalParent(${{i}},'clicked')">${{r.clicked}} <span class="rate">(${{r.click_rate}}%)</span></span>` : `${{r.clicked}} <span class="rate">(${{r.click_rate}}%)</span>`;
    const rCell = r.replied > 0 ? `<span class="clickable" ondblclick="openModalParent(${{i}},'replied')">${{r.replied}} <span class="rate">(${{r.reply_rate}}%)</span></span>` : `${{r.replied}} <span class="rate">(${{r.reply_rate}}%)</span>`;
    html += `<tr>
      <td><strong>${{esc(r.client)}}</strong></td>
      <td style="color:#64748b;font-size:11px;">${{esc(r.raw_name)}}</td>
      <td><span class="badge" style="background:${{sc}}">${{r.status}}</span></td>
      <td>${{r.total}}</td>
      <td>${{oCell}}</td>
      <td>${{cCell}}</td>
      <td>${{rCell}}</td>
      <td style="color:#059669">${{r.positive > 0 ? `<span class="clickable" style="color:#059669" ondblclick="openModalParent(${{i}},'positive')">${{r.positive}} <span class="rate">(${{r.positive_rate}}%)</span></span>` : `${{r.positive}} <span class="rate">(${{r.positive_rate}}%)</span>`}}</td>
      <td style="color:#2563eb">${{r.added_to_sub}} <span class="rate">(${{r.added_to_sub_rate}}%)</span></td>
      <td>${{r.bounced}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  document.getElementById('mainTableContainer').innerHTML = html;
}}

function openModalParent(rowIdx, type) {{
  const row = filtParent[rowIdx];
  const leads = (row.leads || []).filter(l => {{
    if (type === 'opened')   return l.open_time;
    if (type === 'clicked')  return l.click_time;
    if (type === 'replied')  return l.reply_time;
    if (type === 'positive') return POSITIVE_CATS.has((l.category||'').toLowerCase());
    return false;
  }});
  const typeLabel = type === 'opened' ? '📬 Opened' : type === 'clicked' ? '🖱 Clicked' : type === 'positive' ? '✅ Positive' : '💬 Replied';
  document.getElementById('modalTitle').textContent = typeLabel + ' — ' + esc(row.raw_name);
  window._modalLeads = leads;
  const listEl = document.getElementById('leadList');
  if (!leads.length) {{
    listEl.innerHTML = '<div class="empty-list">No lead details available.</div>';
    document.getElementById('msgPanel').innerHTML = '<div class="placeholder">No leads found.</div>';
  }} else {{
    listEl.innerHTML = leads.map((l, i) => `
      <div class="lead-item" id="li-${{i}}" onclick="showMessages(${{i}})">
        <div class="lead-name">${{esc(l.name || '(No Name)')}}</div>
        <div class="lead-email">${{esc(l.email)}}</div>
        <div class="lead-meta">
          ${{l.open_time  ? '📬 ' + fmtDate(l.open_time)  : ''}}
          ${{l.click_time ? ' 🖱 ' + fmtDate(l.click_time) : ''}}
          ${{l.reply_time ? ' 💬 ' + fmtDate(l.reply_time) : ''}}
        </div>
        ${{l.category ? '<div class="lead-cat">' + esc(l.category) + '</div>' : ''}}
      </div>`).join('');
    document.getElementById('msgPanel').innerHTML = '<div class="placeholder">← Select a lead</div>';
  }}
  document.getElementById('modalOverlay').classList.add('open');
}}

// ── Subsequence ───────────────────────────────────────────────────────────────
function renderSubCards() {{
  const totL  = filtSub.reduce((s,r) => s + r.total,   0);
  const totO  = filtSub.reduce((s,r) => s + r.opened,  0);
  const totC  = filtSub.reduce((s,r) => s + r.clicked, 0);
  const totR  = filtSub.reduce((s,r) => s + r.replied, 0);
  const totB  = filtSub.reduce((s,r) => s + r.bounced, 0);
  const avgO  = totL ? (totO/totL*100).toFixed(1) : 0;
  const avgC  = totL ? (totC/totL*100).toFixed(1) : 0;
  document.getElementById('subCards').innerHTML = `
    <div class="card"><div class="label">Total Leads</div><div class="value slate">${{totL.toLocaleString()}}</div><div class="sub">${{filtSub.length}} subsequences</div></div>
    <div class="card"><div class="label">Opened</div><div class="value green">${{totO.toLocaleString()}}</div><div class="sub">Avg ${{avgO}}%</div></div>
    <div class="card"><div class="label">Clicked</div><div class="value blue">${{totC.toLocaleString()}}</div><div class="sub">Avg ${{avgC}}%</div></div>
    <div class="card"><div class="label">Replied</div><div class="value purple">${{totR.toLocaleString()}}</div><div class="sub">&nbsp;</div></div>
    <div class="card"><div class="label">Bounced</div><div class="value yellow">${{totB.toLocaleString()}}</div><div class="sub">&nbsp;</div></div>
  `;
}}

function renderSubCharts() {{
  const active  = filtSub.filter(r => r.total > 0);
  const labels  = active.map(r => r.subsequence);
  const dynH    = id => {{ document.getElementById(id).parentElement.style.height = Math.max(280, active.length * 30) + 'px'; }};
  const hBarOpts = (pct) => ({{
    responsive: true, maintainAspectRatio: false,
    indexAxis: 'y',
    plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: ctx => pct ? ` ${{ctx.parsed.x}}%` : ` ${{ctx.parsed.x}}` }} }} }},
    scales: {{
      x: {{ ticks: {{ color: '#64748b', callback: v => pct ? v+'%' : v }}, grid: {{ color: '#f1f5f9' }}, min: 0, ...(pct ? {{max:100}} : {{}}) }},
      y: {{ ticks: {{ color: '#475569', font: {{ size: 10 }} }}, grid: {{ display: false }} }}
    }}
  }});

  // Open Rate — horizontal bar
  dynH('openChart');
  if (charts['openChart']) charts['openChart'].destroy();
  charts['openChart'] = new Chart(document.getElementById('openChart'), {{
    type: 'bar',
    data: {{ labels, datasets: [{{ data: active.map(r => r.open_rate), backgroundColor: '#22d3ee', borderRadius: 4 }}] }},
    options: hBarOpts(true)
  }});

  // Click Rate — grouped bar (open + click + reply)
  if (charts['clickChart']) charts['clickChart'].destroy();
  charts['clickChart'] = new Chart(document.getElementById('clickChart'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [
        {{ label: 'Open %',  data: active.map(r => r.open_rate),  backgroundColor: '#22d3ee', borderRadius: 3 }},
        {{ label: 'Click %', data: active.map(r => r.click_rate), backgroundColor: '#f43f5e', borderRadius: 3 }},
        {{ label: 'Reply %', data: active.map(r => r.reply_rate), backgroundColor: '#a855f7', borderRadius: 3 }},
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ labels: {{ color: '#475569', font: {{ size: 11 }} }} }}, tooltip: {{ callbacks: {{ label: ctx => ` ${{ctx.dataset.label}}: ${{ctx.parsed.y}}%` }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#64748b', font: {{ size: 10 }}, maxRotation: 45 }}, grid: {{ color: '#f8fafc' }} }},
        y: {{ ticks: {{ color: '#64748b', callback: v => v+'%' }}, grid: {{ color: '#f1f5f9' }}, min: 0, max: 100 }}
      }}
    }}
  }});

  // Reply Rate — horizontal bar
  dynH('replyChart');
  if (charts['replyChart']) charts['replyChart'].destroy();
  charts['replyChart'] = new Chart(document.getElementById('replyChart'), {{
    type: 'bar',
    data: {{ labels, datasets: [{{ data: active.map(r => r.reply_rate), backgroundColor: '#a855f7', borderRadius: 4 }}] }},
    options: hBarOpts(true)
  }});

  // Total Leads — sorted bar
  const sorted = [...active].sort((a,b) => b.total - a.total);
  const BAR_COLORS = ['#f43f5e','#f97316','#eab308','#22c55e','#06b6d4','#3b82f6','#8b5cf6','#ec4899','#14b8a6','#a855f7','#ef4444','#84cc16','#0ea5e9','#fb923c','#4ade80','#38bdf8','#c084fc','#f472b6'];
  if (charts['leadsChart']) charts['leadsChart'].destroy();
  charts['leadsChart'] = new Chart(document.getElementById('leadsChart'), {{
    type: 'bar',
    data: {{
      labels: sorted.map(r => r.subsequence),
      datasets: [{{ data: sorted.map(r => r.total), backgroundColor: sorted.map((_,i) => BAR_COLORS[i % BAR_COLORS.length]), borderRadius: 5 }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ display: false }}, tooltip: {{ callbacks: {{ label: ctx => ` Leads: ${{ctx.parsed.y}}` }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#64748b', font: {{ size: 10 }}, maxRotation: 45 }}, grid: {{ color: '#f8fafc' }} }},
        y: {{ ticks: {{ color: '#64748b' }}, grid: {{ color: '#f1f5f9' }} }}
      }}
    }}
  }});
}}

function renderSubTable() {{
  const container = document.getElementById('subTableContainer');
  if (!filtSub.length) {{ container.innerHTML = '<div class="no-data">No data.</div>'; return; }}
  let html = `<table><thead><tr>
    <th>Client</th><th>Subsequence</th><th>Status</th>
    <th>Leads</th><th>Opened</th><th>Clicked</th><th>Replied</th>
    <th>Bounced</th><th>Unsubscribed</th>
  </tr></thead><tbody>`;
  filtSub.forEach((r, i) => {{
    const sc = r.status === 'ACTIVE' ? '#22c55e' : '#94a3b8';
    const oCell = r.opened  > 0 ? `<span class="clickable" ondblclick="openModal(${{i}},'opened')">${{r.opened}} <span class="rate">(${{r.open_rate}}%)</span></span>`  : `0 <span class="rate">(0%)</span>`;
    const cCell = r.clicked > 0 ? `<span class="clickable" ondblclick="openModal(${{i}},'clicked')">${{r.clicked}} <span class="rate">(${{r.click_rate}}%)</span></span>` : `0 <span class="rate">(0%)</span>`;
    const rCell = r.replied > 0 ? `<span class="clickable" ondblclick="openModal(${{i}},'replied')">${{r.replied}} <span class="rate">(${{r.reply_rate}}%)</span></span>` : `0 <span class="rate">(0%)</span>`;
    html += `<tr>
      <td><strong>${{esc(r.parent)}}</strong></td>
      <td>${{esc(r.subsequence)}}</td>
      <td><span class="badge" style="background:${{sc}}">${{r.status}}</span></td>
      <td>${{r.total}}</td><td>${{oCell}}</td><td>${{cCell}}</td>
      <td>${{rCell}}</td>
      <td>${{r.bounced}}</td><td>${{r.unsubscribed}}</td>
    </tr>`;
  }});
  html += '</tbody></table>';
  container.innerHTML = html;
}}

// ── Modal ─────────────────────────────────────────────────────────────────────
function openModal(rowIdx, type) {{
  const row = filtSub[rowIdx];
  const leads = type === 'opened'  ? (row.leads||[]).filter(l => l.open_time)
              : type === 'clicked' ? (row.leads||[]).filter(l => l.click_time)
              :                      (row.leads||[]).filter(l => l.reply_time);
  const typeLabel = type === 'opened' ? '📬 Opened' : type === 'clicked' ? '🖱 Clicked' : '💬 Replied';
  document.getElementById('modalTitle').textContent = typeLabel + ' — ' + row.subsequence;
  window._modalLeads = leads;
  const listEl = document.getElementById('leadList');
  if (!leads.length) {{
    listEl.innerHTML = '<div class="empty-list">No lead details available.</div>';
    document.getElementById('msgPanel').innerHTML = '<div class="placeholder">No leads found.</div>';
  }} else {{
    listEl.innerHTML = leads.map((l, i) => `
      <div class="lead-item" id="li-${{i}}" onclick="showMessages(${{i}})">
        <div class="lead-name">${{esc(l.name||'(No Name)')}}</div>
        <div class="lead-email">${{esc(l.email)}}</div>
        <div class="lead-meta">
          ${{l.open_time  ? '📬 ' + fmtDate(l.open_time)  : ''}}
          ${{l.click_time ? ' 🖱 ' + fmtDate(l.click_time) : ''}}
          ${{l.reply_time ? ' 💬 ' + fmtDate(l.reply_time) : ''}}
        </div>
        ${{l.category ? '<div class="lead-cat">' + esc(l.category) + '</div>' : ''}}
      </div>`).join('');
    document.getElementById('msgPanel').innerHTML = '<div class="placeholder">← Select a lead</div>';
  }}
  document.getElementById('modalOverlay').classList.add('open');
}}

function showMessages(idx) {{
  document.querySelectorAll('.lead-item').forEach((el,i) => el.classList.toggle('active', i===idx));
  const lead = window._modalLeads[idx];
  const msgs = lead.messages || [];
  const panel = document.getElementById('msgPanel');
  if (!msgs.length) {{ panel.innerHTML = '<div class="placeholder">No message history available.</div>'; return; }}
  panel.innerHTML = '<div class="msg-thread">' + msgs.map(m => {{
    const isReply = m.is_reply === true;
    const bubbleCls = isReply ? 'msg-bubble inbound' : 'msg-bubble';
    const dirBadge  = isReply
      ? '<span class="msg-direction dir-in">↩ Reply from Lead</span>'
      : '<span class="msg-direction dir-out">↗ Sent</span>';
    const tags = [];
    if (!isReply && m.open_time)  tags.push('<span class="tag tag-open">Opened</span>');
    if (!isReply && m.click_time) tags.push('<span class="tag tag-click">Clicked</span>');
    if (isReply)                  tags.push('<span class="tag tag-reply">Reply</span>');
    const timeField = isReply ? m.sent_time : m.sent_time;
    return `<div class="${{bubbleCls}}">
      <div class="msg-meta">
        <span>${{dirBadge}}${{!isReply ? '<span class="msg-seq">Sequence #'+m.seq+'</span>' : ''}}</span>
        <span class="msg-date">${{timeField ? fmtDate(timeField) : ''}}</span>
      </div>
      <div class="msg-subject">${{esc(m.subject||'(No Subject)')}}</div>
      <div class="msg-body">${{esc(m.body||'(No content)')}}</div>
      ${{tags.length ? '<div class="msg-tags">'+tags.join('')+'</div>' : ''}}
    </div>`;
  }}).join('') + '</div>';
}}

function closeModal() {{ document.getElementById('modalOverlay').classList.remove('open'); }}
document.getElementById('modalOverlay').addEventListener('click', e => {{ if (e.target===document.getElementById('modalOverlay')) closeModal(); }});

// ── Helpers ───────────────────────────────────────────────────────────────────
function fmtDate(s) {{
  try {{ return new Date(s).toLocaleString('en-IN', {{dateStyle:'medium',timeStyle:'short'}}); }} catch {{ return s; }}
}}
function esc(s) {{
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}}

// ── Init ──────────────────────────────────────────────────────────────────────
resetFilters();
</script>
</body>
</html>"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    log.info(f"Dashboard saved: {html_path}")


def main():
    log.info("=" * 60)
    log.info(f"Run started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("Fetching campaign analytics...")
    log.info("=" * 60)

    all_campaigns = get_all_campaigns()
    parent_map    = {c["id"]: c for c in all_campaigns}

    # Build parent → subsequences map
    parent_to_subs = defaultdict(list)
    for c in all_campaigns:
        if c.get("parent_campaign_id"):
            parent_to_subs[c["parent_campaign_id"]].append(c)

    # Only process parents that have at least one subsequence
    parents_with_subs = {pid: parent_map[pid] for pid in parent_to_subs if pid in parent_map}
    subsequences      = [c for c in all_campaigns if c.get("parent_campaign_id")]

    log.info(f"Found {len(parents_with_subs)} parent campaign(s) with subsequences")
    log.info(f"Found {len(subsequences)} subsequence(s)")

    # ── Step 1: process subsequences ─────────────────────────────
    csv_rows       = []
    dashboard_data = []
    # Track which emails were added to each parent's subsequences
    parent_sub_emails = defaultdict(set)

    for sub in subsequences:
        sub_id      = sub["id"]
        sub_name    = sub["name"]
        sub_status  = sub.get("status", "UNKNOWN")
        parent      = parent_map.get(sub["parent_campaign_id"], {})
        raw_name    = parent.get("name", f"ID:{sub['parent_campaign_id']}")
        parent_name = resolve_client(raw_name)
        parent_id   = sub["parent_campaign_id"]

        log.info(f"  Sub: [{parent_name}] -> [{sub_name}] (status: {sub_status})")

        stats = get_campaign_stats(sub_id)
        leads = collect_leads_detail(stats)
        leads = enrich_replied_leads(sub_id, leads)
        a     = compute_analytics(leads)

        log.info(
            f"    Leads: {a['total']} | Opens: {a['opened']} ({a['open_rate']}%) | "
            f"Clicks: {a['clicked']} ({a['click_rate']}%) | Replies: {a['replied']} ({a['reply_rate']}%)"
        )

        for l in leads:
            parent_sub_emails[parent_id].add(l['email'])

        dashboard_data.append({
            "run_date":    datetime.now().strftime('%Y-%m-%d'),
            "run_time":    datetime.now().strftime('%H:%M:%S'),
            "parent":      parent_name,
            "subsequence": sub_name,
            "status":      sub_status,
            **a,
            "leads":       leads,
        })

        csv_rows.append({
            "Run Date":        datetime.now().strftime('%Y-%m-%d'),
            "Run Time":        datetime.now().strftime('%H:%M:%S'),
            "Parent Campaign": parent_name,
            "Subsequence":     sub_name,
            "Status":          sub_status,
            "Total Leads":     a["total"],
            "Opened":          a["opened"],
            "Open Rate (%)":   a["open_rate"],
            "Clicked":         a["clicked"],
            "Click Rate (%)":  a["click_rate"],
            "Replied":         a["replied"],
            "Reply Rate (%)":  a["reply_rate"],
            "Bounced":         a["bounced"],
            "Unsubscribed":    a["unsubscribed"],
        })

    # ── Step 2: process parent campaigns ─────────────────────────
    log.info("Fetching parent campaign stats...")
    parent_analytics = []

    for parent_id, parent in parents_with_subs.items():
        raw_name    = parent.get("name", f"ID:{parent_id}")
        client_name = resolve_client(raw_name)
        status      = parent.get("status", "UNKNOWN")

        log.info(f"  Parent: [{client_name}] ({raw_name})")

        stats = get_campaign_stats(parent_id)
        leads = collect_leads_detail(stats)
        leads = enrich_replied_leads(parent_id, leads)
        a     = compute_analytics(leads)

        added_to_sub      = len(parent_sub_emails.get(parent_id, set()))
        added_to_sub_rate = round(added_to_sub / a["total"] * 100, 2) if a["total"] else 0

        log.info(
            f"    Sent: {a['total']} | Opens: {a['opened']} ({a['open_rate']}%) | "
            f"Positive: {a['positive']} ({a['positive_rate']}%) | Added to Sub: {added_to_sub} ({added_to_sub_rate}%)"
        )

        parent_analytics.append({
            "client":            client_name,
            "raw_name":          raw_name,
            "status":            status,
            **a,
            "added_to_sub":      added_to_sub,
            "added_to_sub_rate": added_to_sub_rate,
            "leads":             leads,
        })

    # ── Save outputs ──────────────────────────────────────────────
    os.makedirs("reports", exist_ok=True)

    csv_path = f"reports/subsequence_analytics_{RUN_TIME}.csv"
    fieldnames = [
        "Run Date", "Run Time", "Parent Campaign", "Subsequence", "Status",
        "Total Leads", "Opened", "Open Rate (%)", "Clicked", "Click Rate (%)",
        "Replied", "Reply Rate (%)", "Bounced", "Unsubscribed",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)

    html_path = f"reports/dashboard_{RUN_TIME}.html"
    generate_dashboard(parent_analytics, dashboard_data, html_path)

    log.info("=" * 60)
    log.info(f"Done. CSV: {csv_path}")
    log.info(f"       Dashboard: {html_path}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
