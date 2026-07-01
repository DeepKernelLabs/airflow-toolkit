"""
Demo: v2.4.0 — HTTP operator improvements

Prueba en caliente de las 4 nuevas funcionalidades usando jsonplaceholder.typicode.com
como API pública sin autenticación.

  rate_limited_multi  — MultiHttpToFilesystem secuencial (4 endpoints, 2 req/s)
                        Muestra rate limiting en los logs de Airflow (delay 0.5s entre req)
  concurrent_multi    — MultiHttpToFilesystem paralelo (5 usuarios, max_workers=3)
                        Los 5 requests corren en threads concurrentes
  paginated           — HttpToFilesystem con paginación + 3 req/s de rate limit
                        3 páginas de 5 posts cada una → 3 archivos en MinIO
  oauth2_pattern      — Muestra cómo configurar OAuth2ClientCredentials
                        (no hay servidor OAuth2 en el demo; la tarea valida el setup)
  verify              — Comprueba que los 12 archivos llegaron correctamente a MinIO

Cómo usar:
    docker compose up --build -d
    Airflow UI → http://localhost:8080 → demo_http_v240 → Trigger DAG
    MinIO console → http://localhost:9001  (minio / miniominio)
        raw/demo_http_v240/<ds>/rate_limited/   → 4 archivos .jsonl
        raw/demo_http_v240/<ds>/concurrent/     → 5 archivos .json
        raw/demo_http_v240/<ds>/paginated/      → 3 archivos .jsonl
"""

from __future__ import annotations

import logging
from datetime import timedelta
from urllib.parse import parse_qs, urlparse

from airflow.decorators import dag, task

from airflow_toolkit._compact.airflow_shim import BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.providers.filesystem.operators.auth import OAuth2ClientCredentials
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import (
    HttpToFilesystem,
    MultiHttpToFilesystem,
)

# ── conexiones (pre-configuradas en docker-compose via AIRFLOW_CONN_*) ────────

HTTP_CONN_ID = "demo_http"  # jsonplaceholder.typicode.com  (https)
S3_CONN_ID = "demo_minio"
BUCKET = "raw"

# ── prefijo base en MinIO ─────────────────────────────────────────────────────

BASE_PATH = f"{BUCKET}/demo_http_v240/{{{{ ds }}}}"


# ── función de paginación para JSONPlaceholder ────────────────────────────────
# JSONPlaceholder no devuelve metadatos de paginación en el body, así que
# extraemos el número de página actual de la URL del request y limitamos
# el demo a 3 páginas.


def _jsonplaceholder_paginate(response) -> dict | None:
    """Avanza a la página siguiente; para tras la 3ª (demo)."""
    if not response.json():
        return None
    parsed = parse_qs(urlparse(response.request.url).query)
    current_page = int(parsed.get("_page", ["1"])[0])
    limit = int(parsed.get("_limit", ["5"])[0])
    if current_page >= 3:
        return None
    return {"endpoint": f"/posts?_page={current_page + 1}&_limit={limit}"}


# ── DAG ───────────────────────────────────────────────────────────────────────


@dag(
    dag_id="demo_http_v240",
    schedule=None,
    tags=["demo", "v2.4.0", "http"],
    description="Prueba en caliente: rate limiting · concurrent MultiHttp · paginación · OAuth2",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def demo_http_v240():
    # ── 1. MultiHttpToFilesystem secuencial con rate limiting ─────────────────
    #
    #  4 endpoints en paralelo desde el DAG pero secuenciales dentro del operador,
    #  con un delay de 0.5s entre cada request (requests_per_second=2.0).
    #  En los task logs se verán los 4 requests con su timing.
    #
    #  Resultado: 4 archivos en MinIO
    #    raw/demo_http_v240/<ds>/rate_limited/part0001.jsonl  ← /posts   (5 items)
    #    raw/demo_http_v240/<ds>/rate_limited/part0002.jsonl  ← /comments (5 items)
    #    raw/demo_http_v240/<ds>/rate_limited/part0003.jsonl  ← /albums  (5 items)
    #    raw/demo_http_v240/<ds>/rate_limited/part0004.jsonl  ← /todos   (5 items)

    rate_limited_multi = MultiHttpToFilesystem(
        task_id="rate_limited_multi",
        http_conn_id=HTTP_CONN_ID,
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=f"{BASE_PATH}/rate_limited/",
        endpoint="/posts",
        method="GET",
        save_format="jsonl",
        jmespath_expression="[:5]",
        requests_per_second=2.0,
        multi_requests=[
            {"endpoint": "/posts", "jmespath_expression": "[:5]"},
            {"endpoint": "/comments", "jmespath_expression": "[:5]"},
            {"endpoint": "/albums", "jmespath_expression": "[:5]"},
            {"endpoint": "/todos", "jmespath_expression": "[:5]"},
        ],
        execution_timeout=timedelta(minutes=3),
    )

    # ── 2. MultiHttpToFilesystem paralelo (max_workers=3) ────────────────────
    #
    #  5 requests a /users/{id} (objetos individuales) corren en 3 threads
    #  concurrentes. Cada request escribe su propio archivo con file_number
    #  correcto (sin colisiones).
    #
    #  Resultado: 5 archivos en MinIO
    #    raw/demo_http_v240/<ds>/concurrent/part0001.json  ← user 1
    #    raw/demo_http_v240/<ds>/concurrent/part0002.json  ← user 2
    #    raw/demo_http_v240/<ds>/concurrent/part0003.json  ← user 3
    #    raw/demo_http_v240/<ds>/concurrent/part0004.json  ← user 4
    #    raw/demo_http_v240/<ds>/concurrent/part0005.json  ← user 5

    concurrent_multi = MultiHttpToFilesystem(
        task_id="concurrent_multi",
        http_conn_id=HTTP_CONN_ID,
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=f"{BASE_PATH}/concurrent/",
        endpoint="/users/1",
        method="GET",
        save_format="json",
        max_workers=3,
        multi_requests=[
            {"endpoint": "/users/1"},
            {"endpoint": "/users/2"},
            {"endpoint": "/users/3"},
            {"endpoint": "/users/4"},
            {"endpoint": "/users/5"},
        ],
        execution_timeout=timedelta(minutes=3),
    )

    # ── 3. HttpToFilesystem con paginación + rate limiting ───────────────────
    #
    #  Descarga /posts en páginas de 5 items, con rate limiting de 3 req/s
    #  (delay 0.33s entre páginas). La función de paginación extrae el número
    #  de página actual de la URL del request.
    #
    #  Resultado: 3 archivos en MinIO
    #    raw/demo_http_v240/<ds>/paginated/part0001.jsonl  ← página 1 (5 posts)
    #    raw/demo_http_v240/<ds>/paginated/part0002.jsonl  ← página 2 (5 posts)
    #    raw/demo_http_v240/<ds>/paginated/part0003.jsonl  ← página 3 (5 posts)

    paginated = HttpToFilesystem(
        task_id="paginated_rate_limited",
        http_conn_id=HTTP_CONN_ID,
        filesystem_conn_id=S3_CONN_ID,
        filesystem_path=f"{BASE_PATH}/paginated/",
        endpoint="/posts?_page=1&_limit=5",
        method="GET",
        save_format="jsonl",
        pagination_function=_jsonplaceholder_paginate,
        requests_per_second=3.0,
        execution_timeout=timedelta(minutes=3),
    )

    # ── 4. OAuth2ClientCredentials — demo del patrón ─────────────────────────
    #
    #  No hay servidor OAuth2 en el entorno de demo, así que esta tarea
    #  valida que la clase se construye correctamente y muestra cómo se
    #  usaría en producción (sin hacer ningún HTTP call).
    #
    #  En producción:
    #    from airflow_toolkit.providers.filesystem.operators.auth import OAuth2ClientCredentials
    #
    #    HttpToFilesystem(
    #        auth_type=OAuth2ClientCredentials.client_credentials(
    #            token_url="{{ var.value.oauth2_token_url }}",
    #            client_id="{{ var.value.oauth2_client_id }}",
    #            client_secret="{{ var.value.oauth2_client_secret }}",
    #        ),
    #        ...
    #    )

    @task(task_id="oauth2_pattern")
    def oauth2_pattern_task():
        from requests.auth import AuthBase

        DemoAuth = OAuth2ClientCredentials.client_credentials(
            token_url="https://auth.example.com/oauth2/token",
            client_id="demo_client_id",
            client_secret="demo_client_secret",
            scope="read",
        )

        assert issubclass(DemoAuth, AuthBase), "DemoAuth debe ser subclase de AuthBase"
        assert DemoAuth._token is None, "Token debe iniciar vacío (fetch lazy)"
        assert DemoAuth._expiry == 0.0, "Expiry debe iniciar en 0"

        logging.info("OAuth2ClientCredentials.client_credentials() OK")
        logging.info(
            "Clase generada: %s (subclase de AuthBase: %s)",
            DemoAuth,
            issubclass(DemoAuth, AuthBase),
        )
        logging.info(
            "El token se fetcha en el primer request y se renueva 30s antes de expirar."
        )
        logging.info(
            "En producción: auth_type=OAuth2ClientCredentials.client_credentials(token_url=..., ...)"
        )

    oauth2 = oauth2_pattern_task()

    # ── 5. Verificación final ─────────────────────────────────────────────────
    #
    #  Comprueba que los 12 archivos esperados existen en MinIO.

    @task(task_id="verify")
    def verify_task(**context):
        ds = context["ds"]
        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(S3_CONN_ID)
        )
        base = f"{BUCKET}/demo_http_v240/{ds}"
        errors = []

        # rate_limited_multi → 4 archivos .jsonl
        for i in range(1, 5):
            path = f"{base}/rate_limited/part{i:04}.jsonl"
            if not fs.check_file(path):
                errors.append(f"MISSING: {path}")

        # concurrent_multi → 5 archivos .json
        for i in range(1, 6):
            path = f"{base}/concurrent/part{i:04}.json"
            if not fs.check_file(path):
                errors.append(f"MISSING: {path}")

        # paginated → 3 archivos .jsonl
        for i in range(1, 4):
            path = f"{base}/paginated/part{i:04}.jsonl"
            if not fs.check_file(path):
                errors.append(f"MISSING: {path}")

        if errors:
            raise AssertionError("Faltan archivos en MinIO:\n" + "\n".join(errors))

        logging.info("=== demo_http_v240 verification OK ===")
        logging.info("rate_limited_multi : 4 archivos .jsonl ✓")
        logging.info("concurrent_multi   : 5 archivos .json  ✓")
        logging.info("paginated          : 3 archivos .jsonl ✓")
        logging.info("Total              : 12 archivos en MinIO ✓")

    verify = verify_task()

    # ── dependencias ──────────────────────────────────────────────────────────

    [rate_limited_multi, concurrent_multi, paginated, oauth2] >> verify


demo_http_v240()
