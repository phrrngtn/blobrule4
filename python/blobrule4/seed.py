"""Seed the dataserver table with known ODBC targets."""

import argparse
import json

import sqlalchemy as sa

from blobrule4.models import Base, Dataserver


SEED_DATA = [
    {
        "dataserver_id": 1,
        "name": "sqlserver_docker",
        "driver": "ODBC Driver 18 for SQL Server",
        "host": "localhost",
        "port": 1433,
        "default_catalog": "rule4_test",
        "auth_method": "sql_login",
        "username": "rule4",
        "secret_ref": "env:RULE4_SS_PASSWORD",
        "extra_attrs": json.dumps({"TrustServerCertificate": "yes"}),
        "notes": "SQL Server 2017 on Docker/Rosetta",
    },
    {
        "dataserver_id": 2,
        "name": "pg_local",
        "driver": "PostgreSQL Unicode",
        "host": "/tmp",
        "port": None,
        "default_catalog": "rule4_test",
        "auth_method": "trusted",
        "username": None,
        "secret_ref": None,
        "extra_attrs": json.dumps({"GSSEncMode": "disable"}),
        "notes": "PostgreSQL 17 via Unix socket",
    },
    {
        "dataserver_id": 3,
        "name": "duckdb_memory",
        "driver": "DuckDB Driver",
        "host": None,
        "port": None,
        "default_catalog": ":memory:",
        "auth_method": "none",
        "username": None,
        "secret_ref": None,
        "extra_attrs": None,
        "notes": "DuckDB in-memory (ODBC driver test target)",
    },
]


def main():
    parser = argparse.ArgumentParser(description="Seed dataserver table")
    parser.add_argument("database", help="Path to DuckDB database file")
    args = parser.parse_args()

    engine = sa.create_engine(f"duckdb:///{args.database}")
    Base.metadata.create_all(engine)

    with sa.orm.Session(engine) as session:
        for data in SEED_DATA:
            existing = session.execute(
                sa.select(Dataserver).where(Dataserver.name == data["name"])
            ).scalar_one_or_none()

            if existing is None:
                session.add(Dataserver(**data))
                print(f"  Added: {data['name']}")
            else:
                print(f"  Exists: {data['name']}")

        session.commit()


if __name__ == "__main__":
    main()
