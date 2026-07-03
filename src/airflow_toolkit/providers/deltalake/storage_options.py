from __future__ import annotations

import json

from airflow_toolkit._compact.airflow_shim import Connection


def deltalake_storage_options(connection: Connection) -> dict[str, str]:
    """Maps an Airflow Connection to delta-rs storage_options.

    Supports conn_type "aws" (S3), "wasb" (Azure Blob), and
    "google_cloud_platform" (GCS) — the three backends FilesystemToDeltalake
    can write to.
    """
    if connection.conn_type == "aws":
        return _aws_storage_options(connection)
    elif connection.conn_type == "wasb":
        return _azure_storage_options(connection)
    elif connection.conn_type == "google_cloud_platform":
        return _gcp_storage_options(connection)
    else:
        raise NotImplementedError(
            f"delta-rs storage_options not supported for conn_type "
            f"{connection.conn_type!r}"
        )


def _aws_storage_options(connection: Connection) -> dict[str, str]:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=connection.conn_id)
    credentials = hook.get_credentials()

    options: dict[str, str] = {
        "AWS_ACCESS_KEY_ID": credentials.access_key,
        "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
        # A single Airflow task is the only writer for a given table/run, so
        # there is no concurrent-writer scenario requiring a DynamoDB locking
        # provider — safe to allow the unsafe (non-locked) S3 rename path.
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if credentials.token:
        options["AWS_SESSION_TOKEN"] = credentials.token

    region = hook.conn_region_name
    if region:
        options["AWS_REGION"] = region

    endpoint_url = hook.conn_config.get_service_endpoint_url("s3")
    if endpoint_url:
        options["AWS_ENDPOINT_URL"] = endpoint_url
        # Custom endpoints are typically S3-compatible mocks/MinIO, which speak
        # path-style addressing and are often plain HTTP (e.g. CI's s3mock).
        options["AWS_ALLOW_HTTP"] = str(endpoint_url.startswith("http://")).lower()
        options["AWS_S3_ADDRESSING_STYLE"] = "path"

    return options


def _azure_storage_options(connection: Connection) -> dict[str, str]:
    extra = connection.extra_dejson
    connection_string = extra.get("connection_string")
    if connection_string:
        parsed = _parse_azure_connection_string(connection_string)
        if parsed:
            return parsed

    if not connection.login or not connection.password:
        raise ValueError(
            f"Azure connection {connection.conn_id!r} has neither "
            "'connection_string' in extra nor login/password (account "
            "name/key) set."
        )
    return {
        "azure_storage_account_name": connection.login,
        "azure_storage_account_key": connection.password,
    }


def _parse_azure_connection_string(connection_string: str) -> dict[str, str] | None:
    options: dict[str, str] = {}
    for part in connection_string.split(";"):
        if part.startswith("AccountName="):
            options["azure_storage_account_name"] = part.replace(
                "AccountName=", ""
            ).strip()
        elif part.startswith("AccountKey="):
            options["azure_storage_account_key"] = part.replace(
                "AccountKey=", ""
            ).strip()
    return options or None


def _gcp_storage_options(connection: Connection) -> dict[str, str]:
    extra = connection.extra_dejson
    keyfile_dict = extra.get("keyfile_dict")
    if keyfile_dict:
        key_json = (
            keyfile_dict if isinstance(keyfile_dict, str) else json.dumps(keyfile_dict)
        )
        return {"google_service_account_key": key_json}

    key_path = extra.get("key_path")
    if key_path:
        with open(key_path) as f:
            return {"google_service_account_key": f.read()}

    raise ValueError(
        f"GCP connection {connection.conn_id!r} has neither 'keyfile_dict' "
        "nor 'key_path' set in extra."
    )
