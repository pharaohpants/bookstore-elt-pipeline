param(
    [string]$DagId = "pacbook_postgres_to_snowflake_dbt"
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "Triggering DAG: $DagId"
docker compose exec airflow-webserver airflow dags trigger $DagId

Write-Host "Recent runs for DAG: $DagId"
docker compose exec airflow-webserver airflow dags list-runs -d $DagId
