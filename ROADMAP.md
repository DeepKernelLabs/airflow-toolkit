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

## 6. Notifications

*Following the pattern of `dag_failure_slack_notification_webhook`.*

| Item | Description | Effort |
|------|-------------|--------|
| Microsoft Teams | Native Teams webhook, widely used in enterprise organisations. | S |
| Discord | Popular in smaller technical teams. | S |
| PagerDuty | On-call alerting for critical pipeline failures. | S |
| Generic webhook | Send a configurable JSON payload to any HTTP endpoint. | S |

---

## 7. Developer experience

| Item | Description | Effort |
|------|-------------|--------|
| `MockFilesystem` for unit tests | An in-memory `FilesystemProtocol` implementation — no Docker required for unit tests. | S |
| Remove deprecated code | `DataLakeFacade`, `HttpToDataLake`, `DataLakeDeleteOperator`, `DataLakeCheckOperator` — two API generations currently coexist. | S |
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
| **Critical** — known bugs | `BlobStorageFilesystem.read()`, DuckDB connection string injection, `FilesystemToDatabase` ALTER TABLE quoting |
| **High** — low effort, high value | `FilesystemToDeltalake`, `FilesystemToIceberg`, new sensors, Teams/webhook notifications, remove deprecated code, `list_files()` contract |
| **Medium** — moderate effort, broad adoption | Incremental `FilesystemToFilesystem`, `DeltalakeToFilesystem`, `IcebergToFilesystem`, OAuth refresh, Excel/Avro support, `MockFilesystem`, idempotency |
| **Low** — higher effort or niche use case | GraphQL, FTP, data quality integrations, Snowflake/BigQuery native, `DatabricksSQLToFilesystem` |

---

## 9. Bug fixes & protocol correctness

*Known issues found during code review. These are Jira-ready tasks — each maps to one ticket.*

### BUG-01 · `BlobStorageFilesystem.read()` — invalid `.encode()` on bytes

**File:** `blob_storage_filesystem.py`
**Problem:** `stream.readall()` returns `bytes`, but `.encode()` is called on top of it. Crashes on any Azure Blob read operation.
**Done when:** `.encode()` is removed; existing Azure Blob tests pass; a unit test covers the `read()` return type.
**Effort:** S

---

### BUG-02 · `DuckdbToDeltalake` — connection string interpolated directly into SQL

**File:** `duckdb_to_deltalake.py`
**Problem:** Azure connection strings are f-string interpolated into a DuckDB `CREATE SECRET` statement. A single quote in the key (common in Azure) breaks the SQL. Credentials also leak into DuckDB query logs.
**Done when:** Connection string is properly escaped or passed via DuckDB parameters; a test covers a connection string containing special characters.
**Effort:** S

---

### BUG-03 · `FilesystemToFilesystem` — silent overwrite when destination has no trailing slash

**File:** `filesystem.py`
**Problem:** If `destination_path` has no trailing `/`, it is treated as a literal filename. Copying N source files results in only the last file surviving — no error, no warning.
**Done when:** Operator raises a clear error (or warning) when multiple source files are detected and destination has no trailing slash; behaviour is documented in docstring.
**Effort:** S

---

### BUG-04 · `FilesystemToDatabase` — unquoted table name in `ALTER TABLE`

**File:** `filesystem_to_database.py`
**Problem:** Table name is f-string interpolated into `ALTER TABLE {table_name} ADD COLUMN ...` without SQLAlchemy quoting.
**Done when:** Table and column names are quoted using SQLAlchemy's `quoted_name` or equivalent; a test covers a table name with special characters.
**Effort:** S

---

### PROTOCOL-01 · `list_files()` — inconsistent recursive semantics across filesystems

**Problem:** S3 and GCS return all objects under the prefix (recursive). Local, SFTP, and Azure FileShare return only immediate children. Code written for one backend silently fails on another.
**Done when:** `FilesystemProtocol` documents the expected contract (recursive or not); all implementations are consistent, OR a `recursive: bool` parameter is added and all implementations honour it; tests cover both behaviours.
**Effort:** M

---

### PROTOCOL-02 · Two incompatible `Transformation` Protocol definitions

**Files:** `filesystem.py`, `http_to_filesystem.py`
**Problem:** Both files define a `Transformation` Protocol with different signatures. A callable valid for one operator is not valid for the other.
**Done when:** A single canonical `Transformation` Protocol is defined (e.g. in `protocols.py`); both operators import and use it; existing tests pass.
**Effort:** S

---

### RELIABILITY-01 · `FilesystemToDatabase` — no transaction on bulk insert

**Problem:** Rows are inserted without a wrapping transaction. A failure mid-load leaves partial data in the table with no rollback.
**Done when:** The insert operation is wrapped in a database transaction; on failure the transaction is rolled back and the error is re-raised; a test simulates a mid-load failure and verifies no rows are committed.
**Effort:** M

---

### RELIABILITY-02 · No idempotency — Airflow retries duplicate data

**Problem:** None of the load operators check whether the target already contains data for the current run. An Airflow retry (e.g. after a transient network error) re-inserts everything, duplicating rows.
**Done when:** At least `FilesystemToDatabase` and `SQLToFilesystem` support an idempotency strategy (e.g. `write_disposition: replace | append | skip_if_exists`); the chosen mode is documented and tested.
**Effort:** M

---

### RELIABILITY-03 · `AzureDatabricksVolumeFilesystem.check_file()` — full directory listing to check one file

**File:** `azure_databricks_volume_filesystem.py`
**Problem:** To check whether a single file exists, the implementation lists the entire directory. Slow on large directories; also contains a boolean-list construction that could be simplified.
**Done when:** File existence is checked with a direct `get` or `stat` call where the SDK supports it; fallback to listing uses early exit (`any()` with a generator); a test covers a directory with many entries.
**Effort:** S

---

### DX-01 · Logger names use `__file__` instead of `__name__`

**Problem:** Several modules call `logging.getLogger(__file__)`, producing absolute filesystem paths as logger names in Airflow logs. Makes filtering and log routing difficult.
**Done when:** All `getLogger(__file__)` calls are replaced with `getLogger(__name__)`; no functional change to log messages.
**Effort:** S

---

## Contributing

If you want to pick up any of these items, open an issue referencing this roadmap entry so we can coordinate and avoid duplicate work.

Effort legend: **S** = Small (days) · **M** = Medium (1–2 weeks) · **L** = Large (weeks+)
