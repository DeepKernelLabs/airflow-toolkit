from __future__ import annotations

from airflow_toolkit._compact.airflow_shim import BaseHook


class SharePointHook(BaseHook):
    """
    conn_type: sharepoint
    host: <tenant>.sharepoint.com
    login: <client_id>
    password: <client_secret>
    extra: {"tenant_id": "<tenant_id>", "site_path": "/sites/MySite"}
    """

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        conn = self.get_connection(conn_id)
        self.site_path = conn.extra_dejson.get("site_path", "")
        self.site_url = f"https://{conn.host}{self.site_path}"
        self.client_id = conn.login
        self.client_secret = conn.password

    def get_conn(self):
        from office365.runtime.auth.client_credential import ClientCredential
        from office365.sharepoint.client_context import ClientContext

        return ClientContext(self.site_url).with_credentials(
            ClientCredential(self.client_id, self.client_secret)
        )
