from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.sql.hooks.sql import DbApiHook
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal

from airflow_toolkit._compact.airflow_shim import BaseHook

if TYPE_CHECKING:
    import databricks.sql.client
    from airflow_toolkit._compact.airflow_shim import Connection


class AzureDatabricksSqlHook(DbApiHook):
    """
    Requires defined connection with this structure:
        conn_type: azure_databricks_sql
        host: <hostname>
        login: <service_principal_id>
        password: <service_principal_secret>
        extra: {"http_path": "<http_path>", "catalog": "<catalog>", "schema": "<schema>"}
    """

    conn_name_attr = "azure_databricks_sql_conn_id"
    default_conn_name = "azure_databricks_sql_default"
    conn_type = "azure_databricks_sql"
    hook_name = "Azure Databricks SQL"

    def __init__(
        self,
        azure_databricks_sql_conn_id: str = default_conn_name,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.azure_databricks_sql_conn_id = azure_databricks_sql_conn_id
        self._sql_conn: databricks.sql.client.Connection | None = None

    @cached_property
    def azure_databricks_sql_conn(self) -> "Connection":
        return self.get_connection(self.azure_databricks_sql_conn_id)

    def _get_credentials(self):
        host = self.azure_databricks_sql_conn.host
        if not host:
            raise ValueError(
                f"Connection '{self.azure_databricks_sql_conn_id}' is missing 'host'"
            )
        config = Config(
            host=f"https://{host}",
            client_id=self.azure_databricks_sql_conn.login,
            client_secret=self.azure_databricks_sql_conn.password,
        )
        return oauth_service_principal(config)

    def get_conn(self) -> "databricks.sql.client.Connection":
        if not self._sql_conn:
            self._sql_conn = sql.connect(
                server_hostname=self.azure_databricks_sql_conn.host,
                http_path=self.azure_databricks_sql_conn.extra_dejson.get("http_path"),
                credentials_provider=self._get_credentials,
                catalog=self.azure_databricks_sql_conn.extra_dejson.get("catalog"),
                schema=self.azure_databricks_sql_conn.extra_dejson.get("schema"),
            )
        return self._sql_conn


class AzureDatabricksVolumeHook(BaseHook):
    conn_name_attr = "azure_databricks_volume_conn_id"
    default_conn_name = "azure_databricks_volume_default"
    conn_type = "azure_databricks_volume"
    hook_name = "Azure Databricks Volume"

    def __init__(
        self,
        azure_databricks_volume_conn_id: str = default_conn_name,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.azure_databricks_volume_conn_id = azure_databricks_volume_conn_id
        self.w: WorkspaceClient | None = None

    @cached_property
    def azure_databricks_volume_conn(self) -> "Connection":
        return self.get_connection(self.azure_databricks_volume_conn_id)

    def _get_config(self) -> Config:
        host = self.azure_databricks_volume_conn.host
        if not host:
            raise ValueError(
                f"Connection '{self.azure_databricks_volume_conn_id}' is missing 'host'"
            )
        return Config(
            host=f"https://{host}",
            client_id=self.azure_databricks_volume_conn.login,
            client_secret=self.azure_databricks_volume_conn.password,
        )

    def _get_credentials(self):
        return oauth_service_principal(self._get_config())

    def get_conn(self) -> WorkspaceClient:
        if not self.w:
            host = self.azure_databricks_volume_conn.host
            if not host:
                raise ValueError(
                    f"Connection '{self.azure_databricks_volume_conn_id}' is missing 'host'"
                )
            self.w = WorkspaceClient(host=host, config=self._get_config())
        return self.w
