# Roadmap

This document collects potential improvements and future features for airflow-toolkit. It is not a commitment or a prioritised sprint plan — it serves as a living reference for contributors and maintainers.

Items are grouped by theme and labelled with a rough effort/impact estimate.

---

## 1. New data-movement operators

*These follow the existing ELT philosophy and fill natural gaps in the current operator matrix.*

| Item | Description | Effort |
|------|-------------|--------|
| `FilesystemToDeltalake` | Write files from any filesystem directly to a Delta Lake table. Complements the existing `DuckdbToDeltalake`. | M |
| `DeltalakeToFilesystem` | Export Delta Lake table data to any filesystem (e.g. for downstream consumers). | M |
| `SQLToSQL` / `DatabaseToDatabase` | Cross-database copy without landing on disk (e.g. Postgres → BigQuery). | M |
| `GraphQLToFilesystem` | First-class GraphQL support — query building, variable injection, cursor-based pagination. | M |
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
| XPath-aware XML | Current XML support is basic; add structured extraction via XPath expressions. | M |
| Fixed-width files | Still common in legacy systems (banking, COBOL mainframes). | S |

---

## 6. Delta Lake maintenance operators

*Operational tasks that are necessary in any production Delta Lake deployment.*

| Item | Description | Effort |
|------|-------------|--------|
| `DeltalakeVacuumOperator` | Run `VACUUM` to clean up obsolete files and reclaim storage. | S |
| `DeltalakeOptimizeOperator` | Compact small files to improve read performance. | S |
| `DeltalakeMergeOperator` | UPSERT (MERGE INTO) with a configurable key — the most common SCD Type 1 pattern. | M |
| `DeltalakeSchemaEvolutionOperator` | Apply controlled schema changes to an existing Delta table. | M |

---

## 7. Notifications

*Following the pattern of `dag_failure_slack_notification_webhook`.*

| Item | Description | Effort |
|------|-------------|--------|
| Microsoft Teams | Native Teams webhook, widely used in enterprise organisations. | S |
| Discord | Popular in smaller technical teams. | S |
| PagerDuty | On-call alerting for critical pipeline failures. | S |
| Generic webhook | Send a configurable JSON payload to any HTTP endpoint. | S |

---

## 8. Data quality

*A layer that is currently absent from the toolkit.*

| Item | Description | Effort |
|------|-------------|--------|
| `RowCountCheckOperator` | Assert that a SQL result contains between N and M rows. | S |
| `ColumnNullCheckOperator` | Assert that a column's null rate does not exceed a threshold. | S |
| `SchemaValidationOperator` | Compare a Parquet file or table schema against an expected definition. | M |
| Great Expectations / Soda Core integration | Lightweight wrapper operators to run existing GE/Soda suites from a DAG. | L |

---

## 9. Developer experience

| Item | Description | Effort |
|------|-------------|--------|
| `MockFilesystem` for unit tests | An in-memory `FilesystemProtocol` implementation — no Docker required for unit tests. | S |
| Remove deprecated code | `DataLakeFacade`, `HttpToDataLake`, `DataLakeDeleteOperator`, `DataLakeCheckOperator` — two API generations currently coexist. | S |
| Stricter typing | Use `TypedDict` for `metadata`, `RequestSpec`, and other dict-typed parameters. | S |
| Filesystem scaffolding script | CLI helper to generate the boilerplate for a new filesystem implementation. | S |

---

## 10. Cloud-specific connectors *(longer term)*

| Item | Description | Effort |
|------|-------------|--------|
| BigQuery native operators | `BigQueryToFilesystem` and `FilesystemToBigQuery` beyond what the GCS hook provides. | L |
| Snowflake To/From Filesystem | Common target in modern data stack architectures. | L |
| `DatabricksSQLToFilesystem` | Dedicated operator leveraging the existing `AzureDatabricksSqlHook`. | M |
| AWS Kinesis / Azure Event Hub → Filesystem | Micro-batch streaming ingestion into a data lake. | L |

---

## Priority summary

| Priority | Items |
|----------|-------|
| **High** — low effort, high value | `FilesystemToDeltalake`, Delta Lake maintenance operators, new sensors, Teams/webhook notifications, remove deprecated code |
| **Medium** — moderate effort, broad adoption | Incremental `FilesystemToFilesystem`, OAuth refresh, Excel/Avro support, `MockFilesystem` |
| **Low** — higher effort or niche use case | GraphQL, FTP, data quality integrations, Snowflake/BigQuery native, streaming |

---

## Contributing

If you want to pick up any of these items, open an issue referencing this roadmap entry so we can coordinate and avoid duplicate work.

Effort legend: **S** = Small (days) · **M** = Medium (1–2 weeks) · **L** = Large (weeks+)
