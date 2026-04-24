# smartlead-subsequence-analytics

Generates detailed analytics for Smartlead subsequences — open, click, and reply rates — and publishes a live HTML dashboard to GitHub Pages.

## What it does
- Fetches all subsequences and their parent campaigns
- Computes per-subsequence open/click/reply/bounce metrics
- Fetches message history for replied leads
- Saves a CSV report and an interactive HTML dashboard to `reports/`
- Auto-commits `public/index.html` so GitHub Pages stays current

## Schedule
Runs twice daily: **9:00 AM IST** and **9:00 PM IST**.

## Setup

### 1. Add secret
Go to **Settings → Secrets and variables → Actions** and add:
| Secret | Value |
|--------|-------|
| `SMARTLEAD_API_KEY` | Your Smartlead API key |

### 2. Enable GitHub Pages
Go to **Settings → Pages** and set source to `main` branch, `/public` folder.

### 3. Run locally
```bash
pip install -r requirements.txt
cp .env.example .env   # add your SMARTLEAD_API_KEY
python subsequence_analytics.py
```

## Output
- `reports/subsequence_analytics_<timestamp>.csv`
- `reports/dashboard_<timestamp>.html`
- `public/index.html` (auto-committed by workflow)
