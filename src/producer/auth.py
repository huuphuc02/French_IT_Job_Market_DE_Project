import time
import requests
import logging
from producer.config import FT_TOKEN_URL, FT_CLIENT_ID, FT_CLIENT_SECRET, FT_SCOPE

log = logging.getLogger(__name__)

class TokenManager:
    def __init__(self):
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        if time.time() >= self._expires_at - 30:   # refresh 30 s before expiry
            self._refresh()
        return self._token
 
    def _refresh(self):
        log.info("Fetching new OAuth2 token …")
        resp = requests.post(
            FT_TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     FT_CLIENT_ID,
                "client_secret": FT_CLIENT_SECRET,
                "scope":         FT_SCOPE,
            },
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token     = payload["access_token"]
        self._expires_at = time.time() + payload.get("expires_in", 1400)
        log.info("Token acquired (expires in %s s).", payload.get("expires_in"))