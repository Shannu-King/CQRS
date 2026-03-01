#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE write_db;
    CREATE DATABASE read_db;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "write_db" -f /docker-entrypoint-initdb.d/01-write_db.sql
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "read_db" -f /docker-entrypoint-initdb.d/02-read_db.sql
