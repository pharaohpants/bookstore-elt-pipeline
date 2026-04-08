param(
    [switch]$Rebuild
)

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Update values before running production workloads."
}

Write-Host "Initializing Airflow metadata database and admin user..."
docker compose up airflow-init

Write-Host "Starting services..."
if ($Rebuild) {
    docker compose up -d --build
} else {
    docker compose up -d
}

Write-Host "Current service status:"
docker compose ps
