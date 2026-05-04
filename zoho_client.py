"""
Zoho CRM API client with automatic OAuth2 token refresh.

Required .env vars:
    ZOHO_CLIENT_ID       - from Zoho API Console
    ZOHO_CLIENT_SECRET   - from Zoho API Console
    ZOHO_REFRESH_TOKEN   - generated via OAuth2 authorization flow
    ZOHO_API_DOMAIN      - optional, default: https://www.zohoapis.com
                           EU: https://www.zohoapis.eu
                           IN: https://www.zohoapis.in
    ZOHO_ACCOUNTS_URL    - optional, default: https://accounts.zoho.com
"""

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()


class ZohoClient:
    def __init__(self):
        self.client_id = os.getenv("ZOHO_CLIENT_ID")
        self.client_secret = os.getenv("ZOHO_CLIENT_SECRET")
        self.refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
        self.api_domain = os.getenv("ZOHO_API_DOMAIN", "https://www.zohoapis.com").rstrip("/")
        self.accounts_url = os.getenv("ZOHO_ACCOUNTS_URL", "https://accounts.zoho.com")

        if not all([self.client_id, self.client_secret, self.refresh_token]):
            raise ValueError(
                "ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, and ZOHO_REFRESH_TOKEN "
                "are required in .env"
            )

        self._access_token = None
        self._token_expiry = 0.0
        self.session = requests.Session()

    def _get_access_token(self):
        r = requests.post(
            f"{self.accounts_url}/oauth/v2/token",
            params={
                "grant_type": "refresh_token",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "refresh_token": self.refresh_token,
            },
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        if "access_token" not in data:
            raise RuntimeError(f"Zoho token refresh failed: {data}")
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600) - 60

    def _headers(self) -> dict:
        if not self._access_token or time.time() >= self._token_expiry:
            self._get_access_token()
        return {
            "Authorization": f"Zoho-oauthtoken {self._access_token}",
            "Content-Type": "application/json",
        }

    def upsert_contacts(self, records: list, duplicate_check_fields: list = None) -> dict:
        """Upsert up to 100 Contact records. Returns Zoho response dict."""
        url = f"{self.api_domain}/crm/v2/Contacts/upsert"
        body = {"data": records}
        if duplicate_check_fields:
            body["duplicate_check_fields"] = duplicate_check_fields
        r = self.session.post(url, json=body, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def upsert_leads(self, records: list, duplicate_check_fields: list = None) -> dict:
        """Upsert up to 100 Lead records."""
        url = f"{self.api_domain}/crm/v2/Leads/upsert"
        body = {"data": records}
        if duplicate_check_fields:
            body["duplicate_check_fields"] = duplicate_check_fields
        r = self.session.post(url, json=body, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def add_note(self, parent_id: str, module: str, title: str, content: str) -> dict:
        """Attach a note to any CRM record."""
        url = f"{self.api_domain}/crm/v2/Notes"
        body = {"data": [{
            "Note_Title": title,
            "Note_Content": content,
            "Parent_Id": parent_id,
            "$se_module": module,
        }]}
        r = self.session.post(url, json=body, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()

    def search_records(self, module: str, criteria: str) -> list:
        """Search CRM records by ZOHO criteria string, e.g. (LinkedIn_URN:equals:urn:li:...)"""
        url = f"{self.api_domain}/crm/v2/{module}/search"
        r = self.session.get(
            url,
            params={"criteria": criteria},
            headers=self._headers(),
            timeout=15,
        )
        if r.status_code == 204:
            return []
        r.raise_for_status()
        return r.json().get("data", [])
