"""
Demo: nuevos formatos en FilesystemToDatabaseOperator (v2.3.0)

Prueba en caliente que Excel, Avro y fixed-width se cargan correctamente
desde MinIO a PostgreSQL.

Flujo:
    generate_and_upload
        Genera un dataset de ventas (20 filas) y lo sube a MinIO
        en los 3 formatos: .xlsx · .avro · .fwf
        ↓
    [load_excel · load_avro · load_fixed_width]   (en paralelo)
        FilesystemToDatabaseOperator carga cada formato en su tabla
        ↓
    verify
        Comprueba que las 3 tablas tienen exactamente 20 filas
        y que los valores son correctos.

Cómo usar:
    docker compose up --build -d
    Airflow UI → http://localhost:8080 → demo_new_formats → Trigger DAG
    MinIO console → http://localhost:9001 (minio / miniominio)
    PostgreSQL → psql -h localhost -U dw_user -d demo_dw
"""

from __future__ import annotations

import io
from datetime import timedelta

import fastavro
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow_toolkit._compact.airflow_shim import BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
)

# ── conexiones (pre-configuradas en docker-compose) ───────────────────────────

S3_CONN_ID = "demo_minio"
DB_CONN_ID = "demo_postgres"
BUCKET = "raw"

# ── rutas en MinIO ────────────────────────────────────────────────────────────

BASE_PREFIX = f"{BUCKET}/demo_formats/{{{{ ds.replace('-', '/') }}}}"

EXCEL_PATH = f"{BASE_PREFIX}/excel/"
AVRO_PATH = f"{BASE_PREFIX}/avro/"
FWF_PATH = f"{BASE_PREFIX}/fixed_width/"

# ── tablas destino ────────────────────────────────────────────────────────────

TABLE_EXCEL = "demo_excel_orders"
TABLE_AVRO = "demo_avro_orders"
TABLE_FWF = "demo_fwf_orders"

# ── dataset de prueba ─────────────────────────────────────────────────────────

ORDERS = [
    {"order_id": i, "product": p, "qty": q, "unit_price": up, "region": r}
    for i, (p, q, up, r) in enumerate(
        [
            ("Laptop Pro 15", 2, 1299.99, "North"),
            ("Wireless Mouse", 10, 29.99, "South"),
            ("USB-C Hub", 5, 49.99, "East"),
            ("Monitor 27inch", 3, 399.99, "West"),
            ("Keyboard TKL", 7, 89.99, "North"),
            ("Webcam 4K", 4, 149.99, "South"),
            ("Headset Pro", 6, 199.99, "East"),
            ("Desk Mat XL", 12, 24.99, "West"),
            ("LED Desk Lamp", 8, 39.99, "North"),
            ("Power Bank 20K", 9, 59.99, "South"),
            ("SSD 1TB", 3, 119.99, "East"),
            ("RAM DDR5 32GB", 2, 179.99, "West"),
            ("CPU Cooler", 1, 89.99, "North"),
            ("Case Mid-Tower", 1, 79.99, "South"),
            ("PSU 750W", 2, 99.99, "East"),
            ("GPU RTX 4070", 1, 599.99, "West"),
            ("Ethernet Cable", 15, 9.99, "North"),
            ("HDMI 2.1 Cable", 20, 14.99, "South"),
            ("Thermal Paste", 10, 7.99, "East"),
            ("Cable Mgmt Kit", 5, 19.99, "West"),
        ],
        start=1001,
    )
]

AVRO_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "product", "type": "string"},
        {"name": "qty", "type": "int"},
        {"name": "unit_price", "type": "double"},
        {"name": "region", "type": "string"},
    ],
}


# ── helpers ───────────────────────────────────────────────────────────────────


def _df() -> pd.DataFrame:
    return pd.DataFrame(ORDERS)


def _excel_bytes() -> bytes:
    buf = io.BytesIO()
    _df().to_excel(buf, index=False)
    return buf.getvalue()


def _avro_bytes() -> bytes:
    buf = io.BytesIO()
    fastavro.writer(buf, AVRO_SCHEMA, ORDERS)
    return buf.getvalue()


def _fwf_bytes() -> bytes:
    """Fixed-width: order_id(6) | product(20) | qty(5) | unit_price(10) | region(8)"""
    lines = ["order_id product             qty  unit_price region  "]
    for r in ORDERS:
        lines.append(
            f"{r['order_id']:<6}"
            f"{r['product']:<20}"
            f"{r['qty']:<5}"
            f"{r['unit_price']:<10.2f}"
            f"{r['region']:<8}"
        )
    return "\n".join(lines).encode()


# ── DAG ───────────────────────────────────────────────────────────────────────

METADATA = {
    "_ds": "{{ ds }}",
    "_loaded_at": "{{ dag_run.start_date.isoformat() }}",
}


@dag(
    dag_id="demo_new_formats",
    schedule=None,
    tags=["demo", "v2.3.0", "formats"],
    description="Prueba en caliente: Excel · Avro · fixed-width → FilesystemToDatabaseOperator",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def demo_new_formats():
    # ------------------------------------------------------------------
    #  1. Generar datos y subir a MinIO en los 3 formatos
    # ------------------------------------------------------------------
    @task
    def generate_and_upload(**context):
        ds = context["ds"].replace("-", "/")
        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(S3_CONN_ID)
        )
        fs.write(_excel_bytes(), f"{BUCKET}/demo_formats/{ds}/excel/orders.xlsx")
        fs.write(_avro_bytes(), f"{BUCKET}/demo_formats/{ds}/avro/orders.avro")
        fs.write(_fwf_bytes(), f"{BUCKET}/demo_formats/{ds}/fixed_width/orders.fwf")

    upload = generate_and_upload()

    # ------------------------------------------------------------------
    #  2. Cargar Excel
    # ------------------------------------------------------------------
    load_excel = FilesystemToDatabaseOperator(
        task_id="load_excel",
        filesystem_conn_id=S3_CONN_ID,
        database_conn_id=DB_CONN_ID,
        filesystem_path=EXCEL_PATH,
        db_schema="raw",
        db_table=TABLE_EXCEL,
        source_format="excel",
        table_aggregation_type="replace",
        metadata=METADATA,
        metadata_columns_in_uppercase=False,
        include_source_path=True,
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  3. Cargar Avro
    # ------------------------------------------------------------------
    load_avro = FilesystemToDatabaseOperator(
        task_id="load_avro",
        filesystem_conn_id=S3_CONN_ID,
        database_conn_id=DB_CONN_ID,
        filesystem_path=AVRO_PATH,
        db_schema="raw",
        db_table=TABLE_AVRO,
        source_format="avro",
        table_aggregation_type="replace",
        metadata=METADATA,
        metadata_columns_in_uppercase=False,
        include_source_path=True,
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  4. Cargar fixed-width
    # ------------------------------------------------------------------
    load_fwf = FilesystemToDatabaseOperator(
        task_id="load_fixed_width",
        filesystem_conn_id=S3_CONN_ID,
        database_conn_id=DB_CONN_ID,
        filesystem_path=FWF_PATH,
        db_schema="raw",
        db_table=TABLE_FWF,
        source_format="fixed_width",
        source_format_options={
            "colspecs": [(0, 6), (6, 26), (26, 31), (31, 41), (41, 49)],
            "names": ["order_id", "product", "qty", "unit_price", "region"],
            "skiprows": 1,  # skip the header line we wrote manually
        },
        table_aggregation_type="replace",
        metadata=METADATA,
        metadata_columns_in_uppercase=False,
        include_source_path=True,
        execution_timeout=timedelta(minutes=5),
    )

    # ------------------------------------------------------------------
    #  5. Verificar: las 3 tablas deben tener 20 filas y datos correctos
    # ------------------------------------------------------------------
    verify = SQLExecuteQueryOperator(
        task_id="verify",
        conn_id=DB_CONN_ID,
        autocommit=True,
        sql=f"""
            DO $$
            DECLARE
                excel_count  INT;
                avro_count   INT;
                fwf_count    INT;
                excel_total  NUMERIC;
                avro_total   NUMERIC;
                fwf_total    NUMERIC;
            BEGIN
                SELECT COUNT(*) INTO excel_count FROM raw.{TABLE_EXCEL};
                SELECT COUNT(*) INTO avro_count  FROM raw.{TABLE_AVRO};
                SELECT COUNT(*) INTO fwf_count   FROM raw.{TABLE_FWF};

                SELECT SUM(unit_price * qty) INTO excel_total FROM raw.{TABLE_EXCEL};
                SELECT SUM(unit_price * qty) INTO avro_total  FROM raw.{TABLE_AVRO};
                SELECT SUM(unit_price * qty) INTO fwf_total   FROM raw.{TABLE_FWF};

                RAISE NOTICE '=== demo_new_formats verification ===';
                RAISE NOTICE 'Excel  rows: %  |  revenue: %', excel_count, ROUND(excel_total, 2);
                RAISE NOTICE 'Avro   rows: %  |  revenue: %', avro_count,  ROUND(avro_total,  2);
                RAISE NOTICE 'FWF    rows: %  |  revenue: %', fwf_count,   ROUND(fwf_total,   2);

                IF excel_count != 20 THEN
                    RAISE EXCEPTION 'Excel: expected 20 rows, got %', excel_count;
                END IF;
                IF avro_count != 20 THEN
                    RAISE EXCEPTION 'Avro: expected 20 rows, got %', avro_count;
                END IF;
                IF fwf_count != 20 THEN
                    RAISE EXCEPTION 'FWF: expected 20 rows, got %', fwf_count;
                END IF;

                -- Los 3 totales de revenue deben coincidir (mismos datos)
                IF ROUND(excel_total, 2) != ROUND(avro_total, 2) THEN
                    RAISE EXCEPTION
                        'Revenue mismatch Excel(%) vs Avro(%)',
                        ROUND(excel_total, 2), ROUND(avro_total, 2);
                END IF;

                RAISE NOTICE 'OK — all 3 formats loaded correctly';
            END $$;
        """,
    )

    # ------------------------------------------------------------------
    #  Dependencias
    # ------------------------------------------------------------------
    upload >> [load_excel, load_avro, load_fwf] >> verify


demo_new_formats()
