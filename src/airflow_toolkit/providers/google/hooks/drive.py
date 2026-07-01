from __future__ import annotations

from airflow_toolkit._compact.airflow_shim import BaseHook


class GoogleDriveHook(BaseHook):
    """
    conn_type: google_drive
    extra: {
        "keyfile_json": "{...service account JSON...}",
        "shared_drive_id": "<drive-id>"   # optional; omit for personal Drive
    }
    """

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        conn = self.get_connection(conn_id)
        self.keyfile_json: str = conn.extra_dejson.get("keyfile_json", "")
        self.shared_drive_id: str | None = (
            conn.extra_dejson.get("shared_drive_id") or None
        )

    def get_service(self):
        import json

        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        creds = Credentials.from_service_account_info(
            json.loads(self.keyfile_json),
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        return build("drive", "v3", credentials=creds, cache_discovery=False)
