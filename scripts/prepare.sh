#!/bin/bash

set -e

PGHOST="localhost"
PGPORT=5432
PGUSER="validator"
PGPASSWORD="val1dat0r"
DBNAME="project-sem-1"

export PGHOST PGPORT PGUSER PGPASSWORD DBNAME

echo "Waiting for PostgreSQL to be ready"
for i in {1..10}; do
    if psql -U validator -h localhost -p 5432 -d project-sem-1 -c "\\q" &> /dev/null; then
        echo "PostgreSQL is ready!"
        break
    fi
    echo "PostgreSQL is not ready. sleep ($i/10)"
    sleep 2
done

if ! psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -d "$DBNAME" -c "\\q" &> /dev/null; then
    echo "Database project-sem-1 is not accessible."

    echo "Trying to connect as postgres"
    PGUSER="postgres"
    if ! psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -c "\\q" &> /dev/null; then
        echo "Error: Could not connect to PostgreSQL as postgres."
        exit 1
    fi

    echo "Creating user and db"
    psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" <<-EOSQL
    DO \$\$ BEGIN
      IF NOT EXISTS (SELECT FROM pg_catalog.pg_user WHERE usename = 'validator') THEN
        CREATE USER validator WITH PASSWORD 'val1dat0r';
      END IF;
    END \$\$;

    DO \$\$ BEGIN
      IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'project-sem-1') THEN
        CREATE DATABASE "project-sem-1" OWNER validator;
      END IF;
    END \$\$;

    GRANT ALL PRIVILEGES ON DATABASE "project-sem-1" TO validator;
EOSQL
else
    echo "Database project-sem-1 is accessible. No changes required."
fi

echo "Creating the table prices"
PGUSER="validator"
psql -U "$PGUSER" -h "$PGHOST" -p "$PGPORT" -d "$DBNAME" <<-EOSQL
  CREATE TABLE IF NOT EXISTS prices (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    created_at DATE NOT NULL,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price NUMERIC(10, 2) NOT NULL
  );
EOSQL

echo "Everything setted up"