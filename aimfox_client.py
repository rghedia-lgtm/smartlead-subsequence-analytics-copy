import os
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.aimfox.com/api/v2"


class AimfoxClient:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("AIMFOX_API_KEY")
        if not self.api_key:
            raise ValueError("AIMFOX_API_KEY is required. Set it in .env or pass it directly.")
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _get(self, path: str, params: dict = None) -> dict:
        r = self.session.get(f"{BASE_URL}{path}", params=params, timeout=15)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, data: dict = None) -> dict:
        r = self.session.post(f"{BASE_URL}{path}", json=data, timeout=15)
        r.raise_for_status()
        return r.json()

    # ── Accounts ──────────────────────────────────────────────────────────────

    def list_accounts(self) -> list:
        return self._get("/accounts").get("accounts", [])

    def get_account_limits(self, account_id: str) -> dict:
        return self._get(f"/accounts/{account_id}/limits").get("limit", {})

    # ── Campaigns ─────────────────────────────────────────────────────────────

    def list_campaigns(self) -> list:
        return self._get("/campaigns").get("campaigns", [])

    def get_campaign(self, campaign_id: str) -> dict:
        return self._get(f"/campaigns/{campaign_id}").get("campaign", {})

    def get_campaign_custom_variables(self, campaign_id: str) -> dict:
        """Returns keys + list of {target_urn, variables} for every lead in campaign."""
        return self._get(f"/campaigns/{campaign_id}/custom-variables")

    # ── Analytics ─────────────────────────────────────────────────────────────

    def get_recent_leads(self) -> list:
        """Returns recent lead events: transition = 'accepted' | 'reply' | ..."""
        return self._get("/analytics/recent-leads").get("leads", [])

    # ── Leads ─────────────────────────────────────────────────────────────────

    def get_lead(self, lead_id: str) -> dict:
        return self._get(f"/leads/{lead_id}").get("lead", {})

    def search_leads(self, limit: int = 100, offset: int = 0, **filters) -> list:
        body = {"limit": limit, "offset": offset, **filters}
        return self._post("/leads:search", body).get("leads", [])

    def get_lead_notes(self, lead_id: str) -> list:
        return self._get(f"/leads/{lead_id}/notes").get("notes", [])

    def get_lead_custom_variables(self, account_id: str, lead_urn: str) -> dict:
        return self._get(f"/accounts/{account_id}/leads/{lead_urn}/custom-variables")

    # ── Conversations & Messages ───────────────────────────────────────────────

    def list_conversations(self) -> list:
        return self._get("/conversations").get("conversations", [])

    def get_conversation_messages(self, account_id: str, conversation_urn: str) -> list:
        """Fetch full message thread for a conversation."""
        return self._get(f"/accounts/{account_id}/conversations/{conversation_urn}").get("messages", [])

    def get_lead_conversation(self, account_id: str, lead_id: str) -> dict:
        return self._get(f"/accounts/{account_id}/leads/{lead_id}/conversation")

    # ── Labels & Templates ────────────────────────────────────────────────────

    def list_labels(self) -> list:
        return self._get("/labels").get("labels", [])

    def list_templates(self) -> list:
        return self._get("/templates").get("templates", [])
