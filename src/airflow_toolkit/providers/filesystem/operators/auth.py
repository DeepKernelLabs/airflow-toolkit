from __future__ import annotations

import time
from typing import TYPE_CHECKING

import requests
from requests.auth import AuthBase

if TYPE_CHECKING:
    from requests import PreparedRequest


class OAuth2ClientCredentials:
    """Factory for OAuth 2.0 Client Credentials auth.

    Returns a configured AuthBase class that fetches and caches tokens,
    refreshing automatically 30 seconds before expiry.

    Usage:
        auth_type=OAuth2ClientCredentials.client_credentials(
            token_url="https://auth.example.com/token",
            client_id="{{ var.value.client_id }}",
            client_secret="{{ var.value.client_secret }}",
        )
    """

    @staticmethod
    def client_credentials(
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str | None = None,
    ) -> type[AuthBase]:
        """Return a configured AuthBase subclass for OAuth 2.0 Client Credentials.

        Each call produces an independent class with its own token cache, so
        multiple operators with different credentials do not share tokens.
        """

        class _OAuth2Auth(AuthBase):
            _token: str | None = None
            _expiry: float = 0.0

            def __call__(self, r: "PreparedRequest") -> "PreparedRequest":
                cls = type(self)
                if cls._token is None or time.time() >= cls._expiry - 30:
                    cls._refresh()
                r.headers["Authorization"] = f"Bearer {cls._token}"
                return r

            @classmethod
            def _refresh(cls) -> None:
                payload: dict[str, str] = {
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                }
                if scope:
                    payload["scope"] = scope
                resp = requests.post(token_url, data=payload)
                resp.raise_for_status()
                data = resp.json()
                cls._token = data["access_token"]
                cls._expiry = time.time() + float(data.get("expires_in", 3600))

        return _OAuth2Auth
