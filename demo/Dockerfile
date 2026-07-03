FROM apache/airflow:3.2.1-python3.12

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        git \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install providers needed for the demo:
#   - amazon: S3 / MinIO
#   - postgres: load into PostgreSQL
#   - http: pull from public APIs
#   - fab: auth manager (required in Airflow 3)
#   - deltalake / pyiceberg: FilesystemToDeltalake / FilesystemToIceberg demos (v3.0.0)
#     sql-postgres extra lets pyiceberg's SqlCatalog use demo_postgres directly,
#     avoiding a separate REST catalog service just for the demo.
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon>=9.15.0" \
    "apache-airflow-providers-postgres>=5" \
    "apache-airflow-providers-http>=5.6.4" \
    "apache-airflow-providers-fab>=3.0.0" \
    "apache-airflow-providers-ftp>=3" \
    "pendulum>=3,<4" \
    "pandas>=2.1.1,<3" \
    "requests>=2.31.0" \
    "jmespath>=1.0.1,<2" \
    "dbt-postgres>=1.9.0,<2" \
    "astronomer-cosmos>=1.11.0,<2" \
    "openpyxl>=3.1" \
    "fastavro>=1.9" \
    "deltalake>=1.6,<2" \
    "pyiceberg[pyarrow,adlfs,sql-postgres]>=0.11,<1"
