"""
Demo: v2.5.0 — Nuevos filesystem backends (FTP, SharePoint, Google Drive)

Prueba en caliente del backend FTP contra el servidor local incluido en docker-compose.
SharePoint y Google Drive son servicios externos (requieren credenciales reales); se
documentan aquí los formatos de conexión para usar en producción.

  write_files    — escribe un CSV y un JSON en /demo/{ds}/ via FTP
  read_files     — los lee de vuelta y verifica contenido
  list_dir       — lista los archivos del prefijo
  check_exists   — check_file() y check_prefix() contra el servidor FTP
  cleanup        — elimina el prefijo completo (delete_prefix)
  verify_factory — valida que FilesystemFactory enruta ftp correctamente

Cómo usar:
    docker compose up --build -d          ← reconstruye con apache-airflow-providers-ftp
    Airflow UI → http://localhost:8080 → demo_filesystem_backends_v250 → Trigger DAG

Conexiones pre-configuradas (via AIRFLOW_CONN_* en docker-compose):
    demo_ftp  →  demo-ftp:21  (user=demo, pass=demo123)

Configuración para backends externos (producción):
─────────────────────────────────────────────────────────────────
  SharePoint:
    conn_type: sharepoint
    host:      <tenant>.sharepoint.com
    login:     <client_id>
    password:  <client_secret>
    extra:     {"tenant_id": "<tenant_id>", "site_path": "/sites/MySite"}
    extra lib: pip install "airflow-toolkit[sharepoint]"

  Google Drive:
    conn_type: google_drive
    extra:     {"keyfile_json": "{...service account JSON...}",
                "shared_drive_id": "<drive-id>"}   # omitir para Drive personal
    extra lib: pip install "airflow-toolkit[google_drive]"
─────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
from datetime import timedelta

from airflow.decorators import dag, task

from airflow_toolkit._compact.airflow_shim import BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory

log = logging.getLogger(__name__)

FTP_CONN_ID = "demo_ftp"


@dag(
    dag_id="demo_filesystem_backends_v250",
    schedule=None,
    tags=["demo", "v2.5.0", "ftp", "filesystem"],
    description="Prueba en caliente: FTP backend · FilesystemProtocol completo",
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
)
def demo_filesystem_backends_v250():
    # ── 1. Escribir archivos en FTP ───────────────────────────────────────────
    #
    #  Usa FilesystemFactory para obtener un FTPFilesystem desde la conexión
    #  demo_ftp y escribe dos archivos en /demo/{ds}/.
    #  Los logs muestran los paths escritos.

    @task(task_id="write_files")
    def write_files(**context):
        ds = context["ds"]
        prefix = f"/demo/{ds}"

        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(FTP_CONN_ID)
        )

        fs.create_prefix(prefix)

        csv_data = "id,nombre,valor\n1,alpha,100\n2,beta,200\n3,gamma,300\n"
        fs.write(csv_data, f"{prefix}/datos.csv")
        log.info("Escrito: %s/datos.csv (%d bytes)", prefix, len(csv_data))

        json_data = (
            f'{{"version": "2.5.0", "backend": "ftp", "fecha": "{ds}", "registros": 3}}'
        )
        fs.write(json_data, f"{prefix}/meta.json")
        log.info("Escrito: %s/meta.json (%d bytes)", prefix, len(json_data))

    # ── 2. Leer y verificar contenido ────────────────────────────────────────
    #
    #  Lee los dos archivos de vuelta y verifica que el contenido es correcto.
    #  Un assertion error aquí indica que la escritura o lectura falló.

    @task(task_id="read_files")
    def read_files(**context):
        ds = context["ds"]
        prefix = f"/demo/{ds}"

        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(FTP_CONN_ID)
        )

        csv_bytes = fs.read(f"{prefix}/datos.csv")
        assert b"alpha" in csv_bytes and b"300" in csv_bytes, "Contenido CSV incorrecto"
        log.info("datos.csv (%d bytes):\n%s", len(csv_bytes), csv_bytes.decode())

        json_bytes = fs.read(f"{prefix}/meta.json")
        assert b"2.5.0" in json_bytes and b"ftp" in json_bytes, (
            "Contenido JSON incorrecto"
        )
        log.info("meta.json (%d bytes):\n%s", len(json_bytes), json_bytes.decode())

        log.info("=== read_files: OK ===")

    # ── 3. Listar archivos del prefijo ────────────────────────────────────────
    #
    #  list_files() recorre el directorio recursivamente y devuelve todos los paths.
    #  En este demo debe devolver exactamente 2 archivos.

    @task(task_id="list_dir")
    def list_dir(**context):
        ds = context["ds"]
        prefix = f"/demo/{ds}"

        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(FTP_CONN_ID)
        )

        files = fs.list_files(prefix)
        log.info("Archivos en %s:", prefix)
        for f in files:
            log.info("  %s", f)

        assert len(files) == 2, f"Se esperaban 2 archivos, encontrados: {len(files)}"
        log.info("=== list_dir: %d archivos ===", len(files))
        return files

    # ── 4. Comprobar existencia ───────────────────────────────────────────────
    #
    #  check_file() comprueba que un archivo existe (usa SIZE en FTP).
    #  check_prefix() comprueba que un directorio existe (usa NLST en FTP).
    #  También verifica que check_file devuelve False para un archivo inexistente.

    @task(task_id="check_exists")
    def check_exists(**context):
        ds = context["ds"]
        prefix = f"/demo/{ds}"

        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(FTP_CONN_ID)
        )

        assert fs.check_prefix(prefix), f"check_prefix FAIL: {prefix}"
        log.info("check_prefix(%s): True ✓", prefix)

        assert fs.check_file(f"{prefix}/datos.csv"), "check_file FAIL: datos.csv"
        log.info("check_file(datos.csv): True ✓")

        assert fs.check_file(f"{prefix}/meta.json"), "check_file FAIL: meta.json"
        log.info("check_file(meta.json): True ✓")

        assert not fs.check_file(f"{prefix}/noexiste.txt"), (
            "check_file FAIL: debería ser False"
        )
        log.info("check_file(noexiste.txt): False ✓")

        log.info("=== check_exists: OK ===")

    # ── 5. Limpieza: eliminar el prefijo completo ─────────────────────────────
    #
    #  delete_prefix() elimina recursivamente todos los archivos y el directorio.
    #  Verificamos después que el directorio ya no existe.

    @task(task_id="cleanup")
    def cleanup(**context):
        ds = context["ds"]
        prefix = f"/demo/{ds}"

        fs = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(FTP_CONN_ID)
        )

        fs.delete_prefix(prefix)
        log.info("delete_prefix(%s): OK", prefix)

        assert not fs.check_prefix(prefix), "El prefijo debería haberse eliminado"
        log.info("Verificado: prefijo eliminado ✓")
        log.info("=== cleanup: OK ===")

    # ── 6. Verificar routing del factory ─────────────────────────────────────
    #
    #  Valida que FilesystemFactory instancia correctamente FTPFilesystem
    #  para conn_type="ftp". No hace conexiones reales.

    @task(task_id="verify_factory")
    def verify_factory():
        from airflow_toolkit.filesystems.impl.ftp_filesystem import FTPFilesystem

        conn = BaseHook.get_connection(FTP_CONN_ID)
        fs = FilesystemFactory.get_data_lake_filesystem(connection=conn)

        assert isinstance(fs, FTPFilesystem), (
            f"Se esperaba FTPFilesystem, obtenido: {type(fs).__name__}"
        )
        log.info("FilesystemFactory → FTPFilesystem ✓ (conn_type=%s)", conn.conn_type)
        log.info("=== verify_factory: OK ===")

    # ── dependencias ─────────────────────────────────────────────────────────

    w = write_files()
    r = read_files()
    ld = list_dir()
    c = check_exists()
    d = cleanup()
    v = verify_factory()

    w >> r >> ld >> c >> d
    v  # independiente — valida factory sin tocar FTP


demo_filesystem_backends_v250()
