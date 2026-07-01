# Airflow Toolkit
<div align="center">
  <!-- Logo -->
  <img src="https://raw.githubusercontent.com/DeepKernelLabs/airflow-toolkit/main/media/images/logo.webp" alt="logo" width="400"/>

  <!-- Add some space between the logo and badges -->
  <br/>

  <!-- Badges -->
  <a href="https://github.com/DeepKernelLabs/airflow-toolkit/actions?query=branch%3Amain">
    <img src="https://github.com/DeepKernelLabs/airflow-toolkit/actions/workflows/lint-and-test.yml/badge.svg?branch=main" alt="Badge 1"/>
  </a>
  <a href="https://opensource.org/licenses/Apache-2.0">
    <img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="Badge 2"/>
  </a>
  <a href="https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue">
    <img src="https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue" alt="Badge 3"/>
  </a>

  <br/><br/>
</div>

Collection of operators, hooks and utilities for building ELT pipelines on Apache Airflow.

---

## Why airflow-toolkit?

### The modern ELT pattern

Every data pipeline should follow a single direction:

```
Source  (API / SFTP / SQL database)
  ↓  Extract
Data Lake  (S3 / Azure Blob / GCS / ADLS / …)
  ↓  Load
Warehouse / Database  (Postgres / Databricks / Snowflake / …)
```

The data lake layer is not optional. Every load leaves an immutable copy of the raw data **before** it reaches the warehouse. This gives you:

- **Free reprocessing** — if a transformation has a bug, re-run it from the raw files without calling the source API again.
- **Full traceability** — every row in the warehouse can be traced back to the exact source file that produced it.
- **Decoupled steps** — extraction and loading are independent tasks; each one can be retried or replaced without affecting the other.

### The N×M operator problem

Apache Airflow's built-in operators are point-to-point: one operator per source/destination pair (`FTPToS3Operator`, `S3ToFTPOperator`, `PostgresToS3Operator`, …). With **N sources** and **M destinations** you end up managing **N×M** operators, each with its own implementation and failure modes. Any cross-cutting change (authentication, retry logic, metadata columns) must be replicated across all of them.

### Our solution

`airflow-toolkit` solves this with two ideas:

1. **A common `FilesystemProtocol`** — a thin, uniform interface over S3, Azure Blob, GCS, ADLS, SFTP, FTP, local filesystem, Databricks Volumes, SharePoint, and Google Drive. Operators talk to the protocol, not to the backend. Swapping backends requires no code change in the DAG.

2. **Operators by technology family** — instead of one operator per combination, we provide generic operators (`XToFilesystem`, `FilesystemToX`) that work with any backend through the protocol. The matrix collapses from N×M to a handful of composable building blocks.

---

## Installation

`airflow-toolkit` requires Python 3.11–3.13 and Apache Airflow 3. Install the `airflow3` extra and only the provider extras you actually use:

```bash
# Databricks only — no cloud provider deps
pip install "airflow-toolkit[airflow3,databricks]"

# S3 + HTTP
pip install "airflow-toolkit[airflow3,amazon,http]"

# Everything
pip install "airflow-toolkit[airflow3-full]"
```

### Available extras

| Extra | Installs | Use when |
|---|---|---|
| `airflow3` | `apache-airflow>=3`, `providers-fab`, `pendulum` | **Required** when combining individual extras |
| `databricks` | `databricks-sdk`, `databricks-sql-connector`, `deltalake`, `pandas` | Databricks Volumes, DuckDB→Delta Lake |
| `amazon` | `providers-amazon` | S3 filesystem backend |
| `google` | `providers-google` | GCS filesystem backend |
| `azure` | `providers-microsoft-azure` | Azure Blob / ADLS filesystem backend |
| `sftp` | `providers-sftp` | SFTP filesystem backend |
| `http` | `providers-http`, `requests`, `jmespath`, `pandas` | `HttpToFilesystem`, `MultiHttpToFilesystem` |
| `duckdb` | `airflow-provider-duckdb` | `DuckdbToDeltalake` operator |
| `sqlite` | `providers-sqlite` | SQLite as source or destination |
| `excel` | `openpyxl` | Excel (`.xlsx` / `.xls`) support in `FilesystemToDatabase` and `HttpToFilesystem` |
| `avro` | `fastavro` | Avro support in `FilesystemToDatabase` and `HttpToFilesystem` |
| `ftp` | `providers-ftp` | FTP filesystem backend |
| `sharepoint` | `Office365-REST-Python-Client` | SharePoint filesystem backend |
| `google_drive` | `google-api-python-client`, `google-auth` | Google Drive filesystem backend |
| `airflow3-full` | all of the above | Quick start / development |

---

## Design principles

- **One data-flow direction:** `source → data lake → warehouse`. Every operation must follow one of two shapes: *Extract* (any source → filesystem) or *Load* (filesystem → warehouse).
- **The filesystem layer is mandatory.** Every load must leave an auditable trace before reaching the warehouse.
- **Out of scope:** direct database-to-database copies (`SQLToSQL`), warehouse maintenance operations (`VACUUM`, `OPTIMIZE`, `MERGE INTO`), data quality checks, and streaming ingestion. These either bypass the lake or belong to a separate tooling layer.

---

## Filesystem Protocol

`FilesystemProtocol` is a common interface implemented for the following backends. The correct implementation is resolved at runtime from the Airflow connection's `conn_type`:

| Backend | `conn_type` | Provider / Extra |
|---|---|---|
| Amazon S3 | `aws` | `apache-airflow-providers-amazon` |
| Azure Blob Storage / ADLS | `wasb` | `apache-airflow-providers-microsoft-azure` |
| Google Cloud Storage | `google_cloud_platform` | `apache-airflow-providers-google` |
| SFTP | `sftp` | `apache-airflow-providers-sftp` |
| FTP | `ftp` | `apache-airflow-providers-ftp` |
| Local filesystem | `fs` | built-in |
| Azure File Share (Service Principal) | `azure_file_share_sp` | this library |
| Databricks Unity Catalog Volume | `azure_databricks_volume` | this library |
| SharePoint | `sharepoint` | this library (`[sharepoint]`) |
| Google Drive | `google_drive` | this library (`[google_drive]`) |

Operators resolve the backend automatically:

```python
# S3 connection example
AIRFLOW_CONN_MY_DATA_LAKE='{"conn_type": "aws", "extra": {"endpoint_url": "https://s3.amazonaws.com"}}'

# Azure Blob connection example
AIRFLOW_CONN_MY_DATA_LAKE='{"conn_type": "wasb", "extra": {"connection_string": "DefaultEndpointsProtocol=https;..."}}'
```

Changing the connection's `conn_type` is all that is needed to switch backends — no operator code changes.

---

## Operators

### HttpToFilesystem

Calls an HTTP endpoint and writes the response to any filesystem. Supports pagination, JMESPath filtering, compression, OAuth 2.0 authentication, rate limiting, and custom response transformations.

```python
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import HttpToFilesystem

HttpToFilesystem(
    task_id='fetch_orders',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/orders/{{ ds }}/',
    endpoint='/api/v1/orders',
    method='GET',
    jmespath_expression='data',   # select the 'data' key from the JSON response
    save_format='jsonl',
)
```

**With cursor-based pagination:**

```python
def next_page(response):
    cursor = response.json().get('next_cursor')
    if not cursor:
        return None
    return {'data': {'cursor': cursor}}

HttpToFilesystem(
    task_id='fetch_events',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/events/{{ ds }}/',
    endpoint='/api/v1/events',
    method='POST',
    data={'start_date': '{{ ds }}'},
    pagination_function=next_page,
    save_format='jsonl',
)
```

**With OAuth 2.0 Client Credentials:**

`OAuth2ClientCredentials.client_credentials()` returns a configured auth class that fetches the token lazily on the first request and refreshes it automatically 30 seconds before expiry — no manual token management required.

```python
from airflow_toolkit.providers.filesystem.operators.auth import OAuth2ClientCredentials

HttpToFilesystem(
    task_id='fetch_protected_data',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/data/{{ ds }}/',
    endpoint='/api/v1/data',
    method='GET',
    save_format='jsonl',
    auth_type=OAuth2ClientCredentials.client_credentials(
        token_url='https://auth.example.com/oauth2/token',
        client_id='{{ var.value.oauth2_client_id }}',
        client_secret='{{ var.value.oauth2_client_secret }}',
        scope='read',           # optional
    ),
)
```

**With rate limiting:**

Use `requests_per_second` to cap how fast paginated requests are sent. This is useful when the API enforces a rate limit.

```python
HttpToFilesystem(
    task_id='fetch_with_rate_limit',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/events/{{ ds }}/',
    endpoint='/api/v1/events',
    method='GET',
    pagination_function=next_page,
    save_format='jsonl',
    requests_per_second=3.0,    # max 3 requests per second between pages
)
```

**Supported response formats:**

`save_format` controls how the response is written to the filesystem. For APIs that return binary formats natively (e.g. a reporting API that streams Excel files), set `source_format` to match the response content type:

| `source_format` / `save_format` | File extension | Notes |
|---|---|---|
| `json` | `.json` | Single JSON object or array |
| `jsonl` | `.jsonl` | Array response written as one record per line |
| `csv` | `.csv` | Raw CSV text from the response |
| `xml` | `.xml` | Raw XML text from the response |
| `parquet` | `.parquet` | Binary passthrough — API must return Parquet bytes |
| `excel` | `.xlsx` | Binary passthrough — API must return Excel bytes (requires `[excel]`) |
| `avro` | `.avro` | Binary passthrough — API must return Avro bytes (requires `[avro]`) |
| `fixed_width` | `.fwf` | Fixed-width text from the response |

All text and JSON formats support gzip/zip compression via the `compression` parameter.

### MultiHttpToFilesystem

Runs multiple HTTP requests in a single Airflow task, saving each response as a separate file. Requests can run **sequentially** (with optional rate limiting) or **in parallel** using a thread pool.

**Sequential with rate limiting:**

```python
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import MultiHttpToFilesystem

MultiHttpToFilesystem(
    task_id='fetch_reference_data',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/reference/{{ ds }}/',
    method='GET',
    save_format='jsonl',
    requests_per_second=2.0,    # max 2 requests per second between calls
    multi_requests=[
        {'endpoint': '/api/v1/categories'},
        {'endpoint': '/api/v1/statuses'},
        {'endpoint': '/api/v1/regions'},
    ],
)
```

**Parallel execution:**

Set `max_workers` to run requests concurrently using a thread pool. Each request writes to its own file — there are no file collisions.

```python
MultiHttpToFilesystem(
    task_id='fetch_users_parallel',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/users/{{ ds }}/',
    method='GET',
    save_format='json',
    max_workers=5,              # up to 5 concurrent threads
    multi_requests=[
        {'endpoint': '/api/v1/users/1'},
        {'endpoint': '/api/v1/users/2'},
        {'endpoint': '/api/v1/users/3'},
        {'endpoint': '/api/v1/users/4'},
        {'endpoint': '/api/v1/users/5'},
    ],
)
```

> Rate limiting (`requests_per_second`) applies only in sequential mode. In parallel mode the thread pool controls concurrency — use `max_workers` to avoid overwhelming the API.

Each entry in `multi_requests` can override any base parameter (`endpoint`, `method`, `headers`, `data`, `jmespath_expression`, `save_format`, `compression`).

### SQLToFilesystem

Runs a SQL query against any `DbApiHook`-compatible database and writes the result as Parquet files to any filesystem.

```python
from airflow_toolkit.providers.filesystem.operators.filesystem import SQLToFilesystem

SQLToFilesystem(
    task_id='export_orders',
    source_sql_conn_id='my_postgres',
    destination_fs_conn_id='my_data_lake',
    sql="SELECT * FROM orders WHERE updated_at::date = '{{ ds }}'",
    destination_path='raw/orders/{{ ds }}/',
)
```

For large tables, use `batch_size` to write multiple Parquet part files:

```python
SQLToFilesystem(
    task_id='export_large_table',
    source_sql_conn_id='my_postgres',
    destination_fs_conn_id='my_data_lake',
    sql='SELECT * FROM events',
    destination_path='raw/events/{{ ds }}/',
    batch_size=100_000,
)
```

### FilesystemToFilesystem

Copies files between any two filesystem backends. Because both sides use `FilesystemProtocol`, switching a backend requires only changing the connection — not the operator.

```python
from airflow_toolkit.providers.filesystem.operators.filesystem import FilesystemToFilesystem

FilesystemToFilesystem(
    task_id='landing_to_raw',
    source_fs_conn_id='sftp_source',       # any supported conn_type
    destination_fs_conn_id='s3_data_lake', # any supported conn_type
    source_path='exports/{{ ds }}/',
    destination_path='raw/exports/{{ ds }}/',
)
```

Replace `sftp_source` with an Azure Blob connection and the rest of the DAG stays unchanged.

An optional `data_transformation` callable lets you process each file in-flight:

```python
def decompress_and_decode(data: bytes, filename: str, context: dict) -> bytes:
    import gzip
    return gzip.decompress(data)

FilesystemToFilesystem(
    task_id='decompress_files',
    source_fs_conn_id='sftp_source',
    destination_fs_conn_id='s3_data_lake',
    source_path='exports/{{ ds }}/',
    destination_path='raw/exports/{{ ds }}/',
    data_transformation=decompress_and_decode,
)
```

### FilesystemToDatabase

Reads files from any filesystem and loads them into any SQLAlchemy-compatible database. Handles schema drift automatically: columns present in the file but missing from the table are added; columns present in the table but missing from the file are filled with `NULL`.

**Supported formats:** `csv`, `json`, `parquet`, `excel`, `avro`, `fixed_width`.

```python
from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import FilesystemToDatabaseOperator

FilesystemToDatabaseOperator(
    task_id='load_orders',
    filesystem_conn_id='my_data_lake',     # any supported conn_type
    database_conn_id='my_postgres',        # any SQLAlchemy-compatible connection
    filesystem_path='raw/orders/{{ ds }}/',
    db_schema='public',
    db_table='orders',
    source_format='csv',                   # 'csv' | 'json' | 'parquet' | 'excel' | 'avro' | 'fixed_width'
    table_aggregation_type='append',       # 'append' | 'replace' | 'fail'
    metadata={
        '_ds':          '{{ ds }}',
        '_loaded_at':   '{{ dag_run.start_date.isoformat() }}',
    },
    include_source_path=True,              # adds _LOADED_FROM column for traceability
)
```

**Excel** (requires the `[excel]` extra):

```python
FilesystemToDatabaseOperator(
    task_id='load_excel_report',
    filesystem_conn_id='my_data_lake',
    database_conn_id='my_postgres',
    filesystem_path='raw/reports/{{ ds }}/',
    db_table='monthly_report',
    source_format='excel',
    source_format_options={'sheet_name': 'Data'},
)
```

**Avro** (requires the `[avro]` extra):

```python
FilesystemToDatabaseOperator(
    task_id='load_avro_events',
    filesystem_conn_id='my_data_lake',
    database_conn_id='my_postgres',
    filesystem_path='raw/events/{{ ds }}/',
    db_table='events',
    source_format='avro',
)
```

**Fixed-width** (no extra required — pandas native):

```python
FilesystemToDatabaseOperator(
    task_id='load_fixed_width',
    filesystem_conn_id='my_data_lake',
    database_conn_id='my_postgres',
    filesystem_path='raw/exports/{{ ds }}/',
    db_table='transactions',
    source_format='fixed_width',
    source_format_options={
        'colspecs': [(0, 10), (10, 25), (25, 35)],
        'names': ['date', 'description', 'amount'],
    },
)
```

Each format is matched by file extension: `.csv`/`.csv.gz`, `.json`/`.json.gz`, `.parquet`/`.parquet.gz`, `.xlsx`/`.xls`, `.avro`, `.fwf`/`.txt`/`.dat`. Files with other extensions in the same prefix are silently skipped.

### DuckdbToDeltalake

Executes a DuckDB SQL query and writes the result directly to a Delta Lake table on Azure storage. Useful for in-process transformations that land results as an open table format.

```python
from airflow_toolkit.providers.deltalake.operators.duckdb_to_deltalake import DuckdbToDeltalakeOperator

DuckdbToDeltalakeOperator(
    task_id='transform_to_delta',
    duckdb_conn_id='my_duckdb',
    delta_lake_conn_id='my_azure_storage',
    source_query="""
        SELECT
            order_id,
            customer_id,
            total_amount,
            created_at::DATE AS order_date
        FROM read_parquet('az://my-container/raw/orders/{{ ds }}/*.parquet')
    """,
    table_path='az://my-container/delta/orders',
    write_mode='append',   # 'append' | 'overwrite' | 'error' | 'ignore'
    extensions=['azure'],
)
```

---

## Sensors

### FilesystemFileSensor

Waits until a file exists in any supported filesystem. The backend is resolved from the connection's `conn_type`.

```python
from airflow_toolkit.providers.deltalake.sensors.filesystem_file import FilesystemFileSensor

FilesystemFileSensor(
    task_id='wait_for_daily_export',
    filesystem_conn_id='my_data_lake',
    source_path='raw/orders/{{ ds }}/_SUCCESS',
    poke_interval=60,
    timeout=3600,
)
```

Supported `conn_type` values: `aws`, `wasb`, `google_cloud_platform`, `sftp`, `ftp`, `fs`, `azure_file_share_sp`, `azure_databricks_volume`, `sharepoint`, `google_drive`.

---

## Hooks

### AzureFileShareServicePrincipalHook

Connects to Azure File Share using a Service Principal (client credentials flow). Use `conn_type: azure_file_share_sp`.

```python
AIRFLOW_CONN_MY_FILE_SHARE='{
  "conn_type": "azure_file_share_sp",
  "host": "<storage-account>.file.core.windows.net",
  "login": "<service-principal-client-id>",
  "password": "<service-principal-secret>",
  "extra": {
    "tenant_id": "<tenant-id>",
    "share_name": "<share-name>",
    "protocol": "https"
  }
}'
```

Once the connection is defined, pass it as `filesystem_conn_id` to any operator.

### AzureDatabricksVolumeHook

Provides access to Databricks Unity Catalog Volumes as a filesystem backend. Use `conn_type: azure_databricks_volume`.

### SharePointHook

Connects to a SharePoint site using a Service Principal (OAuth2 client credentials). Use `conn_type: sharepoint`. Requires the `[sharepoint]` extra.

```python
AIRFLOW_CONN_MY_SHAREPOINT='{
  "conn_type": "sharepoint",
  "host": "<tenant>.sharepoint.com",
  "login": "<client-id>",
  "password": "<client-secret>",
  "extra": {
    "tenant_id": "<tenant-id>",
    "site_path": "/sites/MySite"
  }
}'
```

Paths passed to operators are relative to the SharePoint site root (e.g. `Documents/exports/file.csv`). Internally converted to server-relative URLs (`/sites/MySite/Documents/exports/file.csv`).

### GoogleDriveHook

Connects to Google Drive using a Service Account. Use `conn_type: google_drive`. Requires the `[google_drive]` extra. Supports both personal Drive and Shared Drives (Team Drives).

```python
# Personal Drive
AIRFLOW_CONN_MY_GDRIVE='{
  "conn_type": "google_drive",
  "extra": {
    "keyfile_json": "{\"type\": \"service_account\", ...}"
  }
}'

# Shared Drive (Team Drive)
AIRFLOW_CONN_MY_GDRIVE='{
  "conn_type": "google_drive",
  "extra": {
    "keyfile_json": "{\"type\": \"service_account\", ...}",
    "shared_drive_id": "<drive-id>"
  }
}'
```

Paths are relative to the Drive root (e.g. `exports/2024-01-01/data.csv`). File and folder IDs are resolved by name traversal and cached for the lifetime of a task. Omitting `shared_drive_id` targets personal Drive (root = `"root"`).

### AzureDatabricksSqlHook

A `DbApiHook`-compatible hook for running SQL against Databricks SQL Warehouses via a Service Principal. Use `conn_type: azure_databricks_sql`.

```python
AIRFLOW_CONN_MY_DATABRICKS='{
  "conn_type": "azure_databricks_sql",
  "host": "<workspace>.azuredatabricks.net",
  "login": "<service-principal-client-id>",
  "password": "<service-principal-secret>",
  "extra": {
    "http_path": "/sql/1.0/warehouses/<warehouse-id>",
    "catalog": "my_catalog",
    "schema": "my_schema"
  }
}'
```

Because `AzureDatabricksSqlHook` implements `DbApiHook`, it can be used as `source_sql_conn_id` in `SQLToFilesystem`.

---

## Notifications

Send rich failure notifications to Slack, email, Microsoft Teams, and Discord from a single call. The notification system is built around three ideas:

1. **Context builder** — extracts DAG run metadata (run ID, logical date, schedule, interval, duration, environment) from the Airflow callback context once, and makes it available to all channels.
2. **Channel formatters** — each channel (Slack Block Kit, HTML email, Teams Adaptive Card, Discord embed) formats the same context into the right payload for that platform.
3. **Two usage patterns** — as an `on_failure_callback` (invisible to the graph) or as an explicit `@task` node in the DAG graph.

### Pattern A — `on_failure_callback`

The callback fires automatically when any task in the DAG fails. Nothing appears in the task graph.

```python
from airflow_toolkit.notifications import dag_failure_notification

with DAG(
    'my_pipeline',
    schedule='0 6 * * *',
    on_failure_callback=dag_failure_notification(
        channels=['slack', 'email'],
        environment='PROD',
        slack_webhook_url='https://hooks.slack.com/services/...',
        email_to=['data-team@example.com'],
    ),
):
    ...
```

### Pattern B — explicit task in the graph

`get_failure_notification_task` returns an Airflow task with `trigger_rule='one_failed'`. It fires when any upstream task fails and is **skipped** when all tasks succeed. The notification step is visible in the Airflow UI, has its own logs, and appears in the task history.

```python
from airflow_toolkit.notifications import get_failure_notification_task

with DAG('my_pipeline', schedule='0 6 * * *'):
    extract = ...
    load    = ...

    notify = get_failure_notification_task(
        channels=['slack', 'email'],
        environment='PROD',
        slack_webhook_url='https://hooks.slack.com/services/...',
        email_to=['data-team@example.com'],
    )

    [extract, load] >> notify
```

### Supported channels

| Channel | Parameter | Requires |
|---|---|---|
| `slack` | `slack_webhook_url` | — |
| `email` | `email_to: list[str]`, `email_from` (optional) | Airflow SMTP configured |
| `teams` | `teams_webhook_url` | — |
| `discord` | `discord_webhook_url` | — |

Any combination of channels can be used in a single call. Channels are delivered sequentially in the order listed.

### All parameters

```python
dag_failure_notification(
    channels=['slack', 'email', 'teams', 'discord'],

    # Environment label shown in every notification (DEV / STG / PROD)
    environment='PROD',

    # Slack
    slack_webhook_url='https://hooks.slack.com/services/...',

    # Email
    email_to=['ops@example.com'],
    email_from=None,                 # uses Airflow SMTP default if omitted

    # Teams
    teams_webhook_url='https://outlook.office.com/webhook/...',

    # Discord
    discord_webhook_url='https://discord.com/api/webhooks/...',
)
```

`get_failure_notification_task` accepts the same parameters.

### Environment colours

Each environment maps to a distinct colour across all channels so alerts are recognisable at a glance:

| Environment | Slack | Teams | Discord |
|---|---|---|---|
| `PROD` | 🔴 red | Attention (red) | #ED4245 |
| `STG` | 🟡 yellow | Warning (orange) | #FF8C00 |
| `DEV` | 🟢 green | Good (green) | #57F287 |

---

## Testing Utilities

### MockFilesystem

`MockFilesystem` is an in-memory implementation of `FilesystemProtocol` for unit testing. It requires no Docker, no cloud credentials, and no network — all files are stored in a plain Python dict.

```python
from airflow_toolkit.testing import MockFilesystem

# Pre-load files at construction time
fs = MockFilesystem({
    "raw/orders/2024-01-01/data.csv": b"id,amount\n1,100\n2,200",
})

# Or write files programmatically
fs.write(b"id,amount\n3,300", "raw/orders/2024-01-02/data.csv")

# Inspect the result in assertions
assert fs.check_file("raw/orders/2024-01-01/data.csv")
assert len(fs.list_files("raw/orders/")) == 2
assert fs.files["raw/orders/2024-01-01/data.csv"] == b"id,amount\n1,100\n2,200"
```

Use it to patch `FilesystemFactory.get_data_lake_filesystem` in your operator tests:

```python
from unittest.mock import patch
from airflow_toolkit.testing import MockFilesystem

def test_my_pipeline(tmp_path):
    fs = MockFilesystem({"data/file.csv": b"id,name\n1,Alice"})

    with patch(
        "airflow_toolkit.filesystems.filesystem_factory.FilesystemFactory.get_data_lake_filesystem",
        return_value=fs,
    ):
        # run your operator or task here
        ...
```

`MockFilesystem` implements the full `FilesystemProtocol`: `read`, `write`, `delete_file`, `create_prefix`, `delete_prefix`, `check_file`, `check_prefix`, `list_files`.

---

## Running Tests

### Integration tests

Integration tests install the library in a clean virtual environment and exercise each operator end-to-end. You need Docker running locally.

Start the required containers:

```bash
# S3-compatible mock (Adobe S3Mock)
docker run --rm -p 9090:9090 -e initialBuckets=data_lake -e debug=true adobe/s3mock

# SFTP server
docker run -p 22:22 -d atmoz/sftp test_user:pass:::root_folder
```

Set the required environment variables and run the test suite:

```bash
export AIRFLOW_CONN_DATA_LAKE_TEST='{"conn_type": "aws", "extra": {"endpoint_url": "http://localhost:9090"}}'
export AIRFLOW_CONN_SFTP_TEST='{"conn_type": "sftp", "host": "localhost", "port": 22, "login": "test_user", "password": "pass"}'
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1
export TEST_BUCKET=data_lake
export S3_ENDPOINT_URL=http://localhost:9090

uv run pytest tests/ --junitxml=junit/test-results.xml
```

The CI pipeline ([`.github/workflows/lint-and-test.yml`](.github/workflows/lint-and-test.yml)) runs the full matrix automatically on every push: Airflow 3 / Python 3.11, Airflow 3 / Python 3.12, and Airflow 3 / Python 3.13.

> **Python 3.14:** Airflow 3.2+ supports Python 3.14, but `pyarrow` (a transitive dependency of the `[databricks]` extra via `deltalake`) does not yet publish pre-built wheels for Python 3.14. Until it does, Python 3.14 is excluded from the CI matrix and the official test badge. The package can still be installed on Python 3.14 using extras that do not depend on `pyarrow` (e.g. `[airflow3,http,amazon,sftp]`).
