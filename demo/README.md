# airflow-toolkit demo

End-to-end ELT pipeline using [airflow-toolkit](https://pypi.org/project/airflow-toolkit/) operators.

**Stack:** Apache Airflow 3 · MinIO (S3) · PostgreSQL · dbt (via Cosmos) · Metabase

**Pattern:** `JSONPlaceholder API → MinIO (datalake) → PostgreSQL raw → bronze → silver → gold → Metabase`

---

## Quick start

```bash
# 1. Build and start all services
docker compose up --build -d

# 2. Wait ~2 min for Airflow and Metabase to initialise, then:
#      http://localhost:8080  →  Airflow   (trigger the DAG)
#      http://localhost:3000  →  Metabase  (dashboard ready automatically)
#      http://localhost:8082  →  dbt docs  (lineage graph)
#      http://localhost:9001  →  MinIO     (browse raw files)
```

> After the DAG completes, regenerate the dbt docs catalog to include column types:
> ```bash
> docker compose restart dbt-docs
> ```

---

## Services

| Service    | URL                   | Credentials                              |
|------------|-----------------------|------------------------------------------|
| Airflow    | http://localhost:8080 | `admin` / `admin`                        |
| Metabase   | http://localhost:3000 | `admin@demo.local` / `metabase123`       |
| dbt docs   | http://localhost:8082 | —                                        |
| MinIO      | http://localhost:9001 | `minio` / `miniominio`                   |
| PostgreSQL | `localhost:5432`      | `dw_user` / `dw_password` · DB: `demo_dw` |

---

## Airflow — http://localhost:8080

1. Log in with `admin` / `admin`.
2. Find the DAG **`demo_jsonplaceholder`** (may be paused on first load — toggle it on).
3. Click **Trigger DAG** (▶).
4. Watch the run progress in the Grid or Graph view.
5. The DAG runs three task groups in sequence:
   - **setup** — ensures the MinIO prefix and the `raw` schema exist.
   - **extract** — downloads 5 entities from the JSONPlaceholder API and saves them as CSV in MinIO. If a file already exists it is deleted first (idempotent).
   - **load** — reads each CSV from MinIO and appends it to `raw.*` in PostgreSQL (with partition drop for idempotency).
   - **transform** — runs dbt via Cosmos: `bronze → silver → gold`, with tests after all models.

---

## Metabase — http://localhost:3000

Login: `admin@demo.local` / `metabase123`

On first `docker compose up`, **`metabase-init`** configures everything automatically:
- Connects to the `demo_dw` database (gold schema).
- Creates 3 questions and the dashboard **"airflow-toolkit · User Activity"**.

To open the dashboard: **Home → Dashboards → airflow-toolkit · User Activity**

The dashboard contains:
| Visualisation | Type | Description |
|---|---|---|
| User Activity — Overview | Table | All metrics per user, sorted by completion % |
| Posts by User | Bar chart | Number of posts per user |
| Todo Completion Rate (%) | Bar chart | % of todos completed per user |

> Run the DAG first — the gold table must exist before the questions return data.

---

## dbt docs — http://localhost:8082

The lineage graph shows the full model dependency chain:

```
raw (sources)
  └── bronze.*   deduplication
        └── silver.*   type casting + FK relationships
              └── gold__user_activity   aggregation
```

The docs are generated automatically on startup. After the first DAG run, restart the service to include the full column catalog:

```bash
docker compose restart dbt-docs
```

---

## MinIO — http://localhost:9001

Login: `minio` / `miniominio`

Browse **raw → jsonplaceholder → YYYY → MM → DD → {entity}** to see the raw CSV files extracted from the API.

---

## PostgreSQL — localhost:5432

Connect with any SQL client:

| Field    | Value        |
|----------|--------------|
| Host     | `localhost`  |
| Port     | `5432`       |
| Database | `demo_dw`    |
| User     | `dw_user`    |
| Password | `dw_password`|

Schemas: `raw` · `bronze` · `silver` · `gold`

---

## Pipeline

```
JSONPlaceholder API  (public REST API, no auth)
         │
         ▼  HttpToFilesystem
   MinIO / datalake /
   jsonplaceholder / YYYY/MM/DD / {entity} / part0001.csv
         │
         ▼  FilesystemToDatabaseOperator
   PostgreSQL · raw.{entity}
         │
         ▼  dbt (via Cosmos)
   bronze.{entity}     — dedup: latest partition, one row per id
   silver.{entity}     — cast types, snake_case columns, FK tests
   gold.user_activity  — one row per user: posts / comments / albums / todos
         │
         ▼
   Metabase dashboard
```

---

## Tear down

```bash
# Stop and remove containers, networks and volumes
docker compose down -v
```
