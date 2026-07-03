"""
Demo: FilesystemToDeltalakeOperator (v3.0.0)

Prueba en caliente contra un Delta Lake table real sobre MinIO (S3-compatible),
demostrando las dos propiedades más importantes del operador:

  1. Idempotencia — reprocesar el mismo _DS reemplaza los datos de ese run
     en un único commit atómico, sin duplicar filas ni afectar otros _DS.
  2. Schema evolution — un fichero posterior con una columna nueva la añade
     a la tabla automáticamente (NULL en las filas antiguas).

Flujo:
    upload_batch1
        Sube 20 pedidos (_DS = {{ ds }}) a MinIO
        ↓
    load_batch1
        FilesystemToDeltalakeOperator: primera carga
        ↓
    upload_batch1_corrected
        Sube los mismos 20 pedidos con precios corregidos, mismo _DS
        ↓
    load_batch1_corrected
        Reprocesa con idempotent=True (default) → reemplaza, no duplica
        ↓
    upload_batch2_backfill
        Sube 5 pedidos de un día anterior (_DS = ayer) con una columna
        nueva (discount_pct) que no existía en batch1
        ↓
    load_batch2_backfill
        Escribe con _DS distinto → no toca batch1; añade la columna nueva
        ↓
    verify
        Lee la tabla Delta directamente (DeltaTable) y comprueba:
        conteo de filas, precios corregidos, columna nueva con NULLs
        en las filas antiguas.

Cómo usar:
    docker compose up --build -d
    Airflow UI → http://localhost:8080 → demo_deltalake → Trigger DAG
    MinIO console → http://localhost:9001 (minio / miniominio) → raw/demo_deltalake/
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import pendulum
from airflow.decorators import dag, task

from airflow_toolkit._compact.airflow_shim import BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.providers.deltalake.operators.filesystem_to_deltalake import (
    FilesystemToDeltalakeOperator,
)
from airflow_toolkit.providers.deltalake.storage_options import (
    deltalake_storage_options,
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

BASE_PREFIX = f"{BUCKET}/demo_deltalake/{TODAY}"
BATCH1_PATH = f"{BASE_PREFIX}/batch1/"
BATCH2_PATH = f"{BASE_PREFIX}/batch2_backfill/"
TABLE_PATH = f"{BUCKET}/demo_deltalake/delta_table"

# ── datos de prueba ────────────────────────────────────────────────────────────

ORDERS_BATCH1 = [
    {"order_id": 2001, "product": "Laptop Pro 15", "qty": 2, "unit_price": 1299.99},
    {"order_id": 2002, "product": "Wireless Mouse", "qty": 10, "unit_price": 29.99},
    {"order_id": 2003, "product": "USB-C Hub", "qty": 5, "unit_price": 49.99},
    {"order_id": 2004, "product": "Monitor 27inch", "qty": 3, "unit_price": 399.99},
    {"order_id": 2005, "product": "Keyboard TKL", "qty": 7, "unit_price": 89.99},
]

# Misma orden, precios corregidos — demuestra idempotencia (reemplaza, no duplica)
ORDERS_BATCH1_CORRECTED = [
    {"order_id": 2001, "product": "Laptop Pro 15", "qty": 2, "unit_price": 1199.99},
    {"order_id": 2002, "product": "Wireless Mouse", "qty": 10, "unit_price": 24.99},
    {"order_id": 2003, "product": "USB-C Hub", "qty": 5, "unit_price": 44.99},
    {"order_id": 2004, "product": "Monitor 27inch", "qty": 3, "unit_price": 379.99},
    {"order_id": 2005, "product": "Keyboard TKL", "qty": 7, "unit_price": 79.99},
]

# Backfill de un día anterior, con columna nueva — demuestra schema evolution
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
    dag_id="demo_deltalake",
    schedule=None,
    tags=["demo", "v3.0.0", "deltalake"],
    description="Prueba en caliente: FilesystemToDeltalakeOperator — idempotencia y schema evolution",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def demo_deltalake():
    # ------------------------------------------------------------------
    #  1. Subir batch1 (20 pedidos, _DS = ds)
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
    load_batch1 = FilesystemToDeltalakeOperator(
        task_id="load_batch1",
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=BATCH1_PATH,
        table_path=TABLE_PATH,
        delta_storage_conn_id=S3_CONN_ID,
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
    load_batch1_corrected = FilesystemToDeltalakeOperator(
        task_id="load_batch1_corrected",
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=BATCH1_PATH,
        table_path=TABLE_PATH,
        delta_storage_conn_id=S3_CONN_ID,
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
    # ------------------------------------------------------------------
    load_batch2_backfill = FilesystemToDeltalakeOperator(
        task_id="load_batch2_backfill",
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=BATCH2_PATH,
        table_path=TABLE_PATH,
        delta_storage_conn_id=S3_CONN_ID,
        source_format="csv",
        metadata={"_DS": YESTERDAY},
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  7. Verificar leyendo la tabla Delta directamente
    # ------------------------------------------------------------------
    @task
    def verify():
        from deltalake import DeltaTable

        storage_options = deltalake_storage_options(BaseHook.get_connection(S3_CONN_ID))
        df = DeltaTable(TABLE_PATH, storage_options=storage_options).to_pandas()

        print("=== demo_deltalake verification ===")
        print(df.sort_values("order_id").to_string(index=False))

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


demo_deltalake()
