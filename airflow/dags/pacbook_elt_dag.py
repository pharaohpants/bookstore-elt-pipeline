from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from sqlalchemy import create_engine, text

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

SOURCE_TABLES = [
    "customer",
    "address",
    "address_status",
    "country",
    "customer_address",
    "book",
    "book_author",
    "author",
    "publisher",
    "book_language",
    "shipping_method",
    "order_status",
    "cust_order",
    "order_line",
    "order_history",
]

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")


def _required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value


def _failure_callback(context: dict) -> None:
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    logging.error(
        "Task failed | dag_id=%s task_id=%s run_id=%s",
        dag_run.dag_id if dag_run else "unknown",
        task_instance.task_id if task_instance else "unknown",
        dag_run.run_id if dag_run else "unknown",
    )


def _postgres_connection_uri() -> str:
    host = _required_env("POSTGRES_SOURCE_HOST")
    port = _required_env("POSTGRES_SOURCE_PORT")
    database = _required_env("POSTGRES_SOURCE_DB")
    user = _required_env("POSTGRES_SOURCE_USER")
    password = _required_env("POSTGRES_SOURCE_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


def _snowflake_identifier(var_name: str) -> str:
    value = _required_env(var_name).strip().upper()
    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(f"Invalid Snowflake identifier in {var_name}: {value}")
    return value


def _snowflake_connection():
    connection_args = {
        "account": _required_env("SNOWFLAKE_ACCOUNT"),
        "user": _required_env("SNOWFLAKE_USER"),
        "password": _required_env("SNOWFLAKE_PASSWORD"),
        "warehouse": _required_env("SNOWFLAKE_WAREHOUSE"),
        "database": _snowflake_identifier("SNOWFLAKE_DATABASE"),
    }
    role = os.getenv("SNOWFLAKE_ROLE", "").strip()
    if role:
        connection_args["role"] = role

    return snowflake.connector.connect(**connection_args)


def precheck_configuration() -> None:
    required_envs = [
        "POSTGRES_SOURCE_HOST",
        "POSTGRES_SOURCE_PORT",
        "POSTGRES_SOURCE_DB",
        "POSTGRES_SOURCE_USER",
        "POSTGRES_SOURCE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_RAW_SCHEMA",
        "SNOWFLAKE_DW_SCHEMA",
    ]
    for env_name in required_envs:
        _required_env(env_name)


def ensure_snowflake_objects() -> None:
    database = _snowflake_identifier("SNOWFLAKE_DATABASE")
    raw_schema = _snowflake_identifier("SNOWFLAKE_RAW_SCHEMA")
    dw_schema = _snowflake_identifier("SNOWFLAKE_DW_SCHEMA")
    snapshot_schema = os.getenv("SNOWFLAKE_SNAPSHOT_SCHEMA", "SNAPSHOTS").strip().upper()

    if not IDENTIFIER_PATTERN.match(snapshot_schema):
        raise ValueError(f"Invalid Snowflake identifier in SNOWFLAKE_SNAPSHOT_SCHEMA: {snapshot_schema}")

    query_tag = os.getenv("SNOWFLAKE_QUERY_TAG", "").strip()

    with _snowflake_connection() as connection:
        with connection.cursor() as cursor:
            if query_tag:
                safe_query_tag = query_tag.replace("'", "''")
                cursor.execute(f"alter session set query_tag = '{safe_query_tag}'")

            cursor.execute(f"create database if not exists {database}")
            cursor.execute(f"create schema if not exists {database}.{raw_schema}")
            cursor.execute(f"create schema if not exists {database}.{dw_schema}")
            cursor.execute(f"create schema if not exists {database}.{snapshot_schema}")

    logging.info(
        "Snowflake objects ready: database=%s raw_schema=%s dw_schema=%s snapshot_schema=%s",
        database,
        raw_schema,
        dw_schema,
        snapshot_schema,
    )


def extract_table_to_snowflake(table_name: str) -> None:
    database = _snowflake_identifier("SNOWFLAKE_DATABASE")
    raw_schema = _snowflake_identifier("SNOWFLAKE_RAW_SCHEMA")

    pg_engine = create_engine(_postgres_connection_uri())
    sql = text(f"SELECT * FROM public.{table_name}")

    with pg_engine.connect() as connection:
        result = connection.execute(sql)
        dataframe = pd.DataFrame(result.fetchall(), columns=result.keys())

    dataframe.columns = [column.upper() for column in dataframe.columns]
    dataframe["_ELT_LOADED_AT"] = pd.Timestamp.utcnow().to_pydatetime()

    with _snowflake_connection() as sf_connection:
        success, chunks, rows_loaded, _ = write_pandas(
            sf_connection,
            dataframe,
            table_name=table_name.upper(),
            database=database,
            schema=raw_schema,
            auto_create_table=True,
            overwrite=True,
            quote_identifiers=False,
        )

    if not success:
        raise RuntimeError(f"Failed loading table {table_name} into Snowflake")

    logging.info(
        "Loaded table %s into Snowflake %s.%s.%s with %s rows across %s chunk(s)",
        table_name,
        database,
        raw_schema,
        table_name.upper(),
        rows_loaded,
        chunks,
    )


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": _failure_callback,
}

with DAG(
    dag_id="pacbook_postgres_to_snowflake_dbt",
    default_args=default_args,
    description="ELT pipeline PacBook: PostgreSQL source -> Snowflake raw -> dbt transforms",
    schedule=os.getenv("SCHEDULE_CRON", "0 2 * * *"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["pacbook", "elt", "snowflake", "dbt"],
) as dag:
    precheck_task = PythonOperator(
        task_id="precheck_configuration",
        python_callable=precheck_configuration,
    )

    ensure_snowflake_objects_task = PythonOperator(
        task_id="ensure_snowflake_objects",
        python_callable=ensure_snowflake_objects,
    )

    with TaskGroup(group_id="extract_postgres_to_raw") as extract_group:
        for table in SOURCE_TABLES:
            PythonOperator(
                task_id=f"load_{table}",
                python_callable=extract_table_to_snowflake,
                op_kwargs={"table_name": table},
            )

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir /opt/airflow/dbt",
    )

    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt build --profiles-dir /opt/airflow/dbt --target ${DBT_TARGET:-dev}"
        ),
    )

    precheck_task >> ensure_snowflake_objects_task >> extract_group >> dbt_deps_task >> dbt_build_task
