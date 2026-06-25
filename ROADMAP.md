# Roadmap

This document collects potential improvements and future features for airflow-toolkit. It is not a commitment or a prioritised sprint plan — it serves as a living reference for contributors and maintainers.

Items are grouped by theme and labelled with a rough effort/impact estimate.

---

## Design principles

### What is in scope

The toolkit enforces a single data-flow direction: **source → data lake → warehouse/database** (pure ELT).

Every pattern must follow one of two shapes:
- **Extract:** any source → filesystem (data lake)
- **Load:** filesystem (data lake) → warehouse / open table format

The filesystem layer is mandatory — every load must leave an auditable trace before reaching the warehouse.

### What is out of scope

The following categories are explicitly **not supported**, because they bypass the lake, operate entirely within one system, or represent a separate problem domain:

- **`SQLToSQL` / `DatabaseToDatabase`** — direct cross-database copies skip the lake entirely. Use `SQLToFilesystem` + `FilesystemToDatabase` to preserve an audit trail.
- **Warehouse maintenance operations** — `VACUUM`, `OPTIMIZE`, `MERGE INTO`, schema evolution on Delta Lake or Iceberg tables. These are intra-warehouse concerns; tooling such as Databricks Jobs, dbt, or native SQL is better suited.
- **Data quality / validation operators** — row count checks, null rate checks, schema validation, Great Expectations / Soda Core integrations. This is a separate tooling layer that already has mature solutions; the toolkit should not duplicate it.
- **Streaming ingestion** — micro-batch connectors for Kinesis, Event Hubs, etc. follow a fundamentally different execution model and are out of scope for a batch ELT toolkit.

---

## 1. New data-movement operators

*These follow the existing ELT philosophy and fill natural gaps in the current operator matrix.*

| Item | Description | Effort |
|------|-------------|--------|
| `FilesystemToDeltalake` | Write files from any filesystem directly to a Delta Lake table. Complements the existing `DuckdbToDeltalake`. | M |
| `DeltalakeToFilesystem` | Export Delta Lake table data to any filesystem (e.g. for downstream consumers). | M |
| `FilesystemToIceberg` | Write files from any filesystem to an Apache Iceberg table. Symmetric counterpart to `FilesystemToDeltalake`. | M |
| `IcebergToFilesystem` | Export Apache Iceberg table data to any filesystem. Symmetric counterpart to `DeltalakeToFilesystem`. | M |
| `GraphQLToFilesystem` | First-class GraphQL support — query building, variable injection, cursor-based pagination. | M |
| `FilesystemToWarehouseOperator` | Unified `COPY INTO` abstraction for columnar warehouses (Snowflake, Databricks, Redshift, BigQuery). One operator regardless of target. | L |
| Incremental `FilesystemToFilesystem` | Copy only new/modified files based on timestamp or content hash, avoiding full re-processing of a prefix. | L |

---

## 2. New filesystem implementations

*The `FilesystemProtocol` is already designed for easy extension — these are mostly low-effort wrappers.*

| Item | Description | Effort |
|------|-------------|--------|
| Google Drive | Common in GWorkspace organisations. | S |
| SharePoint / OneDrive | Microsoft equivalent for organisations beyond Azure Blob/FileShare. | M |
| FTP (plain) | Many legacy systems only speak plain FTP, not SFTP. | S |
| HTTP / presigned URLs | Treat S3/GCS presigned URLs as a read-only filesystem source. | S |

---

## 3. New sensors

| Item | Description | Effort |
|------|-------------|--------|
| `FilesystemFileSizeSensor` | Wait until a file exceeds (or does not exceed) a size threshold. Useful to detect truncated files. | S |
| `FilesystemFileAgeSensor` | Wait until a file is newer than N hours/days. | S |
| `SQLRowCountSensor` | Wait until a table contains at least N rows. | S |
| `DeltalakeSensor` | Detect new versions of a Delta Lake table. | M |
| `IcebergSensor` | Detect new snapshots of an Apache Iceberg table. | M |
| `HttpResponseSensor` | Poll an endpoint until a condition is met (e.g. external job completed). | S |

---

## 4. HTTP operator improvements

*`HttpToFilesystem` is already feature-rich; these fill the remaining gaps.*

| Item | Description | Effort |
|------|-------------|--------|
| OAuth 2.0 with automatic token refresh | Current `auth_type` is basic; many APIs need mid-pipeline token renewal. | M |
| Configurable rate limiting | Throttle requests to avoid hitting API rate limits. | S |
| Binary response support | Pass through binary payloads (PDF, images, etc.) without forcing format conversion. | S |
| Concurrent `MultiHttpToFilesystem` | Current implementation is sequential — add opt-in `ThreadPoolExecutor` parallelism. | M |

---

## 5. New data format support

| Item | Description | Effort |
|------|-------------|--------|
| Excel (`.xlsx`) | Add `source_format="excel"` to `FilesystemToDatabase` via pandas. Very common in enterprise environments. | S |
| Avro | Standard format in Kafka/Confluent pipelines. | M |
| ORC | Columnar format common in Hadoop/Hive ecosystems. | M |
| XPath-aware XML | Current XML support is basic; add structured extraction via XPath expressions. | M |
| Fixed-width files | Still common in legacy systems (banking, COBOL mainframes). | S |

---

## 6. Notifications overhaul *(target: v2.2.0)*

*The current Slack notification uses a plain code-block format with limited context. The goal is a notification system that is rich, multi-channel, and easy to extend.*

### Architecture

The core idea is to separate **what information to show** from **how to format it**:

1. **Notification context builder** — a shared function that extracts runtime information from the Airflow callback/task context: `dag_id`, `run_id`, `ds`, `schedule`, `data_interval_start/end`, `duration`, `failed_tasks`, `environment` (inferred from webserver base URL), and `dag_url`. Every channel formatter consumes this same dict.

2. **Channel formatters** — each channel (Slack, email, Teams, etc.) receives the context dict and produces the appropriate payload. No channel-specific logic in the context builder.

3. **Two usage patterns:**
   - `on_failure_callback=dag_failure_notification(channels=["slack", "email"])` — classic Airflow callback, plug-and-play.
   - `@task(trigger_rule='one_failed')` pattern — for DAGs that prefer an explicit notification task in the graph (allows dependencies, XCom, etc.).

### Slack improvements

Replace the current plain code-block message with a rich Block Kit layout: header with environment badge, structured fields (run ID, logical date, schedule, interval, duration), context footer with base URL, and a "View in Airflow" action button.

### Reference implementation

`ksa-insights-cloud-composer` (`ksa_cloud_composer/dags/tasks/notifications.py`) already implements this pattern for Slack + email with rich formatting. Use it as the design reference for the generic version in airflow-toolkit.

### Items

| Item | Description | Effort |
|------|-------------|--------|
| Notification context builder | Shared function to extract DAG/run metadata from Airflow context | S |
| Slack rich notifications | Block Kit layout with header, fields, actions (replaces current plain format) | S |
| Email HTML notifications | Professional HTML template with styled table, CTA button, environment badge | S |
| Microsoft Teams | Adaptive Card via Teams webhook | S |
| Discord | Discord webhook with embed formatting | S |
| PagerDuty | On-call alerting for critical pipeline failures | S |
| Generic webhook | Send a configurable JSON payload to any HTTP endpoint | S |

---

## 7. Developer experience

| Item | Description | Effort |
|------|-------------|--------|
| `MockFilesystem` for unit tests | An in-memory `FilesystemProtocol` implementation — no Docker required for unit tests. | S |
| ~~Remove deprecated code~~ | ~~`DataLakeFacade`, `HttpToDataLake`, `DataLakeDeleteOperator`, `DataLakeCheckOperator` — two API generations currently coexist.~~ **Done in v2.1.0** | S |
| Stricter typing | Use `TypedDict` for `metadata`, `RequestSpec`, and other dict-typed parameters. | S |
| Filesystem scaffolding script | CLI helper to generate the boilerplate for a new filesystem implementation. | S |

---

## 8. Cloud-specific connectors *(longer term)*

| Item | Description | Effort |
|------|-------------|--------|
| BigQuery native operators | `BigQueryToFilesystem` and `FilesystemToBigQuery` beyond what the GCS hook provides. | L |
| Snowflake To/From Filesystem | Common target in modern data stack architectures. | L |
| `DatabricksSQLToFilesystem` | Dedicated operator leveraging the existing `AzureDatabricksSqlHook`. | M |

---

## Priority summary

| Priority | Items |
|----------|-------|
| **High** — low effort, high value | `FilesystemToDeltalake`, `FilesystemToIceberg`, new sensors, Teams/webhook notifications |
| **Medium** — moderate effort, broad adoption | Incremental `FilesystemToFilesystem`, `DeltalakeToFilesystem`, `IcebergToFilesystem`, OAuth refresh, Excel/Avro support, `MockFilesystem` |
| **Low** — higher effort or niche use case | GraphQL, FTP, data quality integrations, Snowflake/BigQuery native, `DatabricksSQLToFilesystem` |

---

## 9. Bug fixes & protocol correctness

*Known issues found during code review. These are Jira-ready tasks — each maps to one ticket.*

### ~~BUG-01 · `BlobStorageFilesystem.read()` — invalid `.encode()` on bytes~~ ✓

~~**File:** `blob_storage_filesystem.py`~~
**Resolved in v2.1.0:** Removed invalid `.encode()` call on `bytes` returned by `stream.readall()`.

---

### ~~BUG-02 · `DuckdbToDeltalake` — connection string interpolated directly into SQL~~ ✓

~~**File:** `duckdb_to_deltalake.py`~~
**Resolved in v2.1.0:** Connection string single quotes are now escaped before interpolation into `CREATE SECRET`.

---

### ~~BUG-03 · `FilesystemToFilesystem` — silent overwrite when destination has no trailing slash~~ ✓

~~**File:** `filesystem.py`~~
**Resolved in v2.1.0:** Operator raises `ValueError` when multiple source files are detected and `destination_path` has no trailing `/`.

---

### ~~BUG-04 · `FilesystemToDatabase` — unquoted table name in `ALTER TABLE`~~ ✓

~~**File:** `filesystem_to_database.py`~~
**Resolved in v2.1.0:** Table name is now quoted with `"` in `ALTER TABLE` statements.

---

### ~~PROTOCOL-01 · `list_files()` — inconsistent recursive semantics across filesystems~~ ✓

**Resolved in v2.1.0:** All implementations now list files recursively. Contract documented in `FilesystemProtocol`. Local uses `rglob("*")`, SFTP uses `get_tree_map()`, Azure FileShare and Databricks Volumes recurse into subdirectories.

---

### ~~PROTOCOL-02 · Two incompatible `Transformation` Protocol definitions~~ ✓

~~**Files:** `filesystem.py`, `http_to_filesystem.py`~~
**Resolved in v2.1.0:** Two clearly named Protocols (`FilesystemTransformation`, `HttpTransformation`) defined in `protocols.py`. Both operators import from the shared module.

---

### ~~RELIABILITY-01 · `FilesystemToDatabase` — no transaction on bulk insert~~ ✓

**Resolved in v2.1.0:** Each file's load is wrapped in `engine.begin()`. On failure, all batches for that file are rolled back. Also fixed `first_batch` being reset per file (which caused `table_aggregation_type="replace"` to drop data from previous files).

---

### ~~RELIABILITY-02 · No idempotency — Airflow retries duplicate data~~ ✓

**Resolved in v2.1.0:** `FilesystemToDatabaseOperator` now supports `idempotent=True` (default). Before loading, rows matching the current run's metadata (e.g. `_DS`) are deleted, preventing duplicates on retry.

---

### ~~RELIABILITY-03 · `AzureDatabricksVolumeFilesystem.check_file()` — full directory listing to check one file~~ ✓

~~**File:** `azure_databricks_volume_filesystem.py`~~
**Resolved in v2.1.0:** Replaced full directory listing with `files.get_metadata(path)` — a single O(1) HEAD request.

---

### ~~DX-01 · Logger names use `__file__` instead of `__name__`~~ ✓

**Resolved in v2.1.0:** All `getLogger(__file__)` calls replaced with `getLogger(__name__)` across 5 modules.

---

## Contributing

If you want to pick up any of these items, open an issue referencing this roadmap entry so we can coordinate and avoid duplicate work.

Effort legend: **S** = Small (days) · **M** = Medium (1–2 weeks) · **L** = Large (weeks+)
