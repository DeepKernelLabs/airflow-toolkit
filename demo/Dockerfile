FROM apache/airflow:3.1.2-python3.12

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
RUN pip install --no-cache-dir \
    "apache-airflow-providers-amazon>=9.15.0" \
    "apache-airflow-providers-postgres>=5" \
    "apache-airflow-providers-http>=5.6.4" \
    "apache-airflow-providers-fab>=3.0.0" \
    "pendulum>=3,<4" \
    "pandas>=2.1.1,<3" \
    "requests>=2.31.0" \
    "dbt-postgres>=1.9.0,<2" \
    "astronomer-cosmos>=1.11.0,<2"
