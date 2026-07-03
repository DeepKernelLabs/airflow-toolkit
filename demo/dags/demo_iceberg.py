"""
Demo: FilesystemToIcebergOperator (v3.0.0)

Prueba en caliente contra una tabla Iceberg real (catálogo SQL sobre
PostgreSQL, datos sobre MinIO/S3), demostrando:

  1. Idempotencia — reprocesar el mismo _DS reemplaza los datos de ese run
     sin duplicar filas ni afectar otros _DS.
  2. Schema evolution manual — a diferencia de Delta, Iceberg no evoluciona
     el schema en el append(); el operador llama a table.update_schema()
     .union_by_name() antes de cada fichero, así que columnas nuevas se
     añaden igualmente sin intervención manual del usuario del operador.
  3. Un snapshot de Iceberg por batch (no por fichero) — visible en
     table.snapshots() al final del DAG.

catalog_properties es un passthrough directo a pyiceberg.catalog.load_catalog()
— aquí se usa un SqlCatalog embebido sobre demo_postgres para no depender de
un servicio de catálogo REST externo, pero cualquier catálogo que soporte
pyiceberg (REST, Glue, Hive...) funciona igual sin cambiar el operador.

Flujo: idéntico a demo_deltalake (mismo dataset, mismo patrón de verificación)
para poder comparar el comportamiento de ambos operadores directamente.

Cómo usar:
    docker compose up --build -d
    Airflow UI → http://localhost:8080 → demo_iceberg → Trigger DAG
    MinIO console → http://localhost:9001 (minio / miniominio) → raw/demo_iceberg/
    psql -h localhost -U dw_user -d demo_dw -c "SELECT * FROM iceberg_tables;"
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task

from airflow_toolkit._compact.airflow_shim import BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.providers.iceberg.operators.filesystem_to_iceberg import (
    FilesystemToIcebergOperator,
)

# ── conexiones (pre-configuradas en docker-compose) ───────────────────────────

S3_CONN_ID = "demo_minio"
BUCKET = "raw"

# Fechas calculadas en Python (no via Jinja {{ ds }}) — este DAG se dispara
# manualmente sin logical_date explícito, y Airflow 3 no rellena `ds` en el
# contexto de una task ni en el render de template_fields en ese caso.
TODAY = pendulum.now("UTC").to_date_string()
YESTERDAY = pendulum.now("UTC").subtract(days=1).to_date_string()

# ── rutas en MinIO ────────────────────────────────────────────────────────────

BASE_PREFIX = f"{BUCKET}/demo_iceberg/{TODAY}"
BATCH1_PATH = f"{BASE_PREFIX}/batch1/"
BATCH2_PATH = f"{BASE_PREFIX}/batch2_backfill/"

# ── catálogo Iceberg — SQL catalog embebido sobre demo_postgres ──────────────
# Credenciales del docker-compose de la demo (ver AIRFLOW_CONN_DEMO_POSTGRES /
# AIRFLOW_CONN_DEMO_MINIO). En un caso real, catalog_properties se rellenaría
# con las credenciales del catálogo/warehouse que corresponda (REST, Glue...).

CATALOG_NAME = "demo_iceberg_catalog"
CATALOG_PROPERTIES = {
    "type": "sql",
    # demo_dw has no "public" schema granted to dw_user (only raw/bronze/silver/gold,
    # see scripts/init-dw.sh) — point search_path at "raw" so SqlCatalog's unqualified
    # CREATE TABLE iceberg_tables lands somewhere dw_user can actually create in.
    "uri": "postgresql+psycopg2://dw_user:dw_password@postgres:5432/demo_dw?options=-csearch_path%3Draw",
    "warehouse": f"s3://{BUCKET}/demo_iceberg/warehouse",
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "minio",
    "s3.secret-access-key": "miniominio",
    "s3.region": "us-east-1",
}
TABLE_IDENTIFIER = "demo.orders"

# ── datos de prueba — mismo dataset que demo_deltalake, para comparar ────────

ORDERS_BATCH1 = [
    {"order_id": 2001, "product": "Laptop Pro 15", "qty": 2, "unit_price": 1299.99},
    {"order_id": 2002, "product": "Wireless Mouse", "qty": 10, "unit_price": 29.99},
    {"order_id": 2003, "product": "USB-C Hub", "qty": 5, "unit_price": 49.99},
    {"order_id": 2004, "product": "Monitor 27inch", "qty": 3, "unit_price": 399.99},
    {"order_id": 2005, "product": "Keyboard TKL", "qty": 7, "unit_price": 89.99},
]

ORDERS_BATCH1_CORRECTED = [
    {"order_id": 2001, "product": "Laptop Pro 15", "qty": 2, "unit_price": 1199.99},
    {"order_id": 2002, "product": "Wireless Mouse", "qty": 10, "unit_price": 24.99},
    {"order_id": 2003, "product": "USB-C Hub", "qty": 5, "unit_price": 44.99},
    {"order_id": 2004, "product": "Monitor 27inch", "qty": 3, "unit_price": 379.99},
    {"order_id": 2005, "product": "Keyboard TKL", "qty": 7, "unit_price": 79.99},
]

ORDERS_BATCH2 = [
    {
        "order_id": 1001,
        "product": "Webcam 4K",
        "qty": 4,
        "unit_price": 149.99,
        "discount_pct": 10,
    },
    {
        "order_id": 1002,
        "product": "Headset Pro",
        "qty": 6,
        "unit_price": 199.99,
        "discount_pct": 0,
    },
]


def _overwrite(fs, path: str, data: bytes) -> None:
    """fs.write() does not replace an existing key — delete first so this
    DAG can be re-triggered manually on the same day without failing."""
    if fs.check_file(path):
        fs.delete_file(path)
    fs.write(data, path)


@dag(
    dag_id="demo_iceberg",
    schedule=None,
    tags=["demo", "v3.0.0", "iceberg"],
    description="Prueba en caliente: FilesystemToIcebergOperator — idempotencia y schema evolution",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def demo_iceberg():
    # ------------------------------------------------------------------
    #  1. Subir batch1 (5 pedidos, _DS = ds)
    # ------------------------------------------------------------------
    @task
    def upload_batch1():
        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(S3_CONN_ID)
        )
        csv_bytes = pd.DataFrame(ORDERS_BATCH1).to_csv(index=False).encode()
        _overwrite(fs, f"{BATCH1_PATH}orders.csv", csv_bytes)

    # ------------------------------------------------------------------
    #  2. Cargar batch1
    # ------------------------------------------------------------------
    load_batch1 = FilesystemToIcebergOperator(
        task_id="load_batch1",
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=BATCH1_PATH,
        catalog_name=CATALOG_NAME,
        catalog_properties=CATALOG_PROPERTIES,
        table_identifier=TABLE_IDENTIFIER,
        source_format="csv",
        metadata={"_DS": TODAY},
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  3. Subir batch1 corregido (mismo _DS, precios distintos)
    # ------------------------------------------------------------------
    @task
    def upload_batch1_corrected():
        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(S3_CONN_ID)
        )
        csv_bytes = pd.DataFrame(ORDERS_BATCH1_CORRECTED).to_csv(index=False).encode()
        _overwrite(fs, f"{BATCH1_PATH}orders.csv", csv_bytes)

    # ------------------------------------------------------------------
    #  4. Recargar batch1 — idempotent=True (default) reemplaza, no duplica
    # ------------------------------------------------------------------
    load_batch1_corrected = FilesystemToIcebergOperator(
        task_id="load_batch1_corrected",
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=BATCH1_PATH,
        catalog_name=CATALOG_NAME,
        catalog_properties=CATALOG_PROPERTIES,
        table_identifier=TABLE_IDENTIFIER,
        source_format="csv",
        metadata={"_DS": TODAY},
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  5. Subir backfill de ayer con columna nueva (discount_pct)
    # ------------------------------------------------------------------
    @task
    def upload_batch2_backfill():
        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(S3_CONN_ID)
        )
        csv_bytes = pd.DataFrame(ORDERS_BATCH2).to_csv(index=False).encode()
        _overwrite(fs, f"{BATCH2_PATH}orders.csv", csv_bytes)

    # ------------------------------------------------------------------
    #  6. Cargar backfill — _DS distinto, no toca batch1; añade columna nueva
    #     via table.update_schema().union_by_name() (automático en el operador)
    # ------------------------------------------------------------------
    load_batch2_backfill = FilesystemToIcebergOperator(
        task_id="load_batch2_backfill",
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=BATCH2_PATH,
        catalog_name=CATALOG_NAME,
        catalog_properties=CATALOG_PROPERTIES,
        table_identifier=TABLE_IDENTIFIER,
        source_format="csv",
        metadata={"_DS": YESTERDAY},
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  7. Verificar leyendo la tabla Iceberg directamente
    # ------------------------------------------------------------------
    @task
    def verify():
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(CATALOG_NAME, **CATALOG_PROPERTIES)
        table = catalog.load_table(TABLE_IDENTIFIER)
        df = table.scan().to_pandas()

        print("=== demo_iceberg verification ===")
        print(df.sort_values("order_id").to_string(index=False))
        print(f"Snapshots: {len(list(table.snapshots()))}")

        if len(df) != 7:
            raise ValueError(f"Expected 7 rows (5 batch1 + 2 batch2), got {len(df)}")

        batch1 = df[df["order_id"] == 2001].iloc[0]
        if batch1["unit_price"] != 1199.99:
            raise ValueError(
                f"Idempotent replace failed: expected corrected price 1199.99, "
                f"got {batch1['unit_price']}"
            )

        if "discount_pct" not in df.columns:
            raise ValueError("Schema evolution failed: discount_pct column missing")

        old_row = df[df["order_id"] == 2002].iloc[0]
        if pd.notna(old_row["discount_pct"]):
            raise ValueError(
                f"Expected NULL discount_pct for pre-existing row, got "
                f"{old_row['discount_pct']}"
            )

        new_row = df[df["order_id"] == 1001].iloc[0]
        if new_row["discount_pct"] != 10:
            raise ValueError(
                f"Expected discount_pct=10 for backfilled row, got {new_row['discount_pct']}"
            )

        print("OK — idempotent replace and schema evolution both verified")

    # ------------------------------------------------------------------
    #  Dependencias
    # ------------------------------------------------------------------
    (
        upload_batch1()
        >> load_batch1
        >> upload_batch1_corrected()
        >> load_batch1_corrected
        >> upload_batch2_backfill()
        >> load_batch2_backfill
        >> verify()
    )


demo_iceberg()
