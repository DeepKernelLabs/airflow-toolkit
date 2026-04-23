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
  <a href="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue">
    <img src="https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12-blue" alt="Badge 3"/>
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

1. **A common `FilesystemProtocol`** — a thin, uniform interface over S3, Azure Blob, GCS, ADLS, SFTP, local filesystem, and Databricks Volumes. Operators talk to the protocol, not to the backend. Swapping backends requires no code change in the DAG.

2. **Operators by technology family** — instead of one operator per combination, we provide generic operators (`XToFilesystem`, `FilesystemToX`) that work with any backend through the protocol. The matrix collapses from N×M to a handful of composable building blocks.

---

## Installation

```bash
pip install airflow-toolkit            # Airflow 2
pip install airflow-toolkit[airflow3]  # Airflow 3
```

---

## Design principles

- **One data-flow direction:** `source → data lake → warehouse`. Every operation must follow one of two shapes: *Extract* (any source → filesystem) or *Load* (filesystem → warehouse).
- **The filesystem layer is mandatory.** Every load must leave an auditable trace before reaching the warehouse.
- **Out of scope:** direct database-to-database copies (`SQLToSQL`), warehouse maintenance operations (`VACUUM`, `OPTIMIZE`, `MERGE INTO`), data quality checks, and streaming ingestion. These either bypass the lake or belong to a separate tooling layer.

---

## Filesystem Protocol

`FilesystemProtocol` is a common interface implemented for the following backends. The correct implementation is resolved at runtime from the Airflow connection's `conn_type`:

| Backend | `conn_type` | Provider |
|---|---|---|
| Amazon S3 | `aws` | `apache-airflow-providers-amazon` |
| Azure Blob Storage / ADLS | `wasb` | `apache-airflow-providers-microsoft-azure` |
| Google Cloud Storage | `google_cloud_platform` | `apache-airflow-providers-google` |
| SFTP | `sftp` | `apache-airflow-providers-sftp` |
| Local filesystem | `fs` | built-in |
| Azure File Share (Service Principal) | `azure_file_share_sp` | this library |
| Databricks Unity Catalog Volume | `azure_databricks_volume` | this library |

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

Calls an HTTP endpoint and writes the response to any filesystem. Supports pagination, JMESPath filtering, compression, and custom response transformations.

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

With cursor-based pagination:

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

### MultiHttpToFilesystem

Runs multiple HTTP requests in a single Airflow task, saving each response as a separate file. Useful for fetching multiple entities or date ranges without creating one task per request.

```python
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import MultiHttpToFilesystem

MultiHttpToFilesystem(
    task_id='fetch_reference_data',
    http_conn_id='my_api',
    filesystem_conn_id='my_data_lake',
    filesystem_path='raw/reference/{{ ds }}/',
    method='GET',
    save_format='jsonl',
    multi_requests=[
        {'endpoint': '/api/v1/categories'},
        {'endpoint': '/api/v1/statuses'},
        {'endpoint': '/api/v1/regions'},
    ],
)
```

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

Reads files (CSV, JSON, or Parquet) from any filesystem and loads them into any SQLAlchemy-compatible database. Handles schema drift automatically: columns present in the file but missing from the table are added; columns present in the table but missing from the file are filled with `NULL`.

```python
from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import FilesystemToDatabaseOperator

FilesystemToDatabaseOperator(
    task_id='load_orders',
    filesystem_conn_id='my_data_lake',     # any supported conn_type
    database_conn_id='my_postgres',        # any SQLAlchemy-compatible connection
    filesystem_path='raw/orders/{{ ds }}/',
    db_schema='public',
    db_table='orders',
    source_format='csv',
    table_aggregation_type='append',       # 'append' | 'replace' | 'fail'
    metadata={
        '_ds':          '{{ ds }}',
        '_loaded_at':   '{{ dag_run.start_date.isoformat() }}',
    },
    include_source_path=True,              # adds _LOADED_FROM column for traceability
)
```

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

Supported `conn_type` values: `aws`, `wasb`, `google_cloud_platform`, `sftp`, `fs`, `azure_file_share_sp`, `azure_databricks_volume`.

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

### Slack (incoming webhook)

Send DAG or task failure alerts to a Slack channel using `dag_failure_slack_notification_webhook`. Requires a Slack App with Incoming Webhooks enabled.

Create an Airflow connection named `SLACK_WEBHOOK_NOTIFICATION_CONN` (or set `AIRFLOW_CONN_SLACK_WEBHOOK_NOTIFICATION_CONN`).

#### DAG-level notification

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow_toolkit.notifications.slack.webhook import dag_failure_slack_notification_webhook

with DAG(
    'my_pipeline',
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=dag_failure_slack_notification_webhook(),
) as dag:

    t = BashOperator(task_id='run', bash_command='python my_script.py')
```

#### Task-level notification

```python
BashOperator(
    task_id='run',
    bash_command='python my_script.py',
    on_failure_callback=dag_failure_slack_notification_webhook(source='TASK'),
)
```

#### Custom message

```python
on_failure_callback=dag_failure_slack_notification_webhook(
    text='Pipeline {{ dag.dag_id }} failed on {{ ds }}',
    include_blocks=False,
)
```

#### Custom Slack blocks

```python
on_failure_callback=dag_failure_slack_notification_webhook(
    blocks={
        'type': 'section',
        'text': {'type': 'mrkdwn', 'text': '*Pipeline failed* — check the logs.'},
    }
)
```

Default notification format:

![image](https://github.com/DeepKernelLabs/airflow-toolkit/assets/152852247/52a5bf95-21bc-4c3b-8093-79953c0c5d61)

**Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `text` | `str` (optional) | Plain-text message. Overrides blocks if provided. |
| `blocks` | `dict` (optional) | Custom Slack Block Kit payload. |
| `include_blocks` | `bool` (optional) | Whether to include the default formatted block. |
| `source` | `'DAG'` \| `'TASK'` (optional) | Source of the failure. Default: `'DAG'`. |
| `image_url` | `str` (optional) | Accessory image URL. Can also be set via `AIRFLOW_TOOLKIT__SLACK_NOTIFICATION_IMG_URL`. |

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

poetry run pytest tests/ --junitxml=junit/test-results.xml
```

The CI pipeline ([`.github/workflows/lint-and-test.yml`](.github/workflows/lint-and-test.yml)) runs the full matrix automatically on every push: Airflow 2 / Python 3.11, Airflow 3 / Python 3.11, and Airflow 3 / Python 3.12.
