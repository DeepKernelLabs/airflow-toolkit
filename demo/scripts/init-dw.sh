#!/bin/bash
# Creates the demo data warehouse database and all layer schemas inside the same PostgreSQL instance.
set -e

echo "Creating demo_dw database..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER dw_user WITH PASSWORD 'dw_password';
    CREATE DATABASE demo_dw OWNER dw_user;
EOSQL

echo "Creating schemas in demo_dw (raw → bronze → silver → gold)..."
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "demo_dw" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE SCHEMA IF NOT EXISTS bronze;
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE SCHEMA IF NOT EXISTS gold;
    GRANT ALL ON SCHEMA raw    TO dw_user;
    GRANT ALL ON SCHEMA bronze TO dw_user;
    GRANT ALL ON SCHEMA silver TO dw_user;
    GRANT ALL ON SCHEMA gold   TO dw_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA raw    GRANT ALL ON TABLES TO dw_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES TO dw_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES TO dw_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES TO dw_user;
EOSQL

echo "demo_dw ready (schemas: raw, bronze, silver, gold)."
