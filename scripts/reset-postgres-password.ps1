#Requires -RunAsAdministrator
# Resets the local PostgreSQL 18 'postgres' password when it has been forgotten,
# and creates the 'forms' database if it is missing.
#
# How: temporarily allow passwordless logins from this machine only (127.0.0.1 / ::1),
# set the new password, then ALWAYS restore the original pg_hba.conf from a backup.
#
# Usage (PowerShell as Administrator, in the project folder):
#   powershell -ExecutionPolicy Bypass -File scripts\reset-postgres-password.ps1
param(
    [string]$NewPassword = "sneha@123",
    [string]$PgHome      = "C:\Program Files\PostgreSQL\18",
    [string]$Service     = "postgresql-x64-18"
)
$ErrorActionPreference = "Stop"

$hba    = Join-Path $PgHome "data\pg_hba.conf"
$psql   = Join-Path $PgHome "bin\psql.exe"
$backup = "$hba.bak-" + (Get-Date -Format "yyyyMMddHHmmss")

Write-Host "Backing up pg_hba.conf -> $backup"
Copy-Item $hba $backup

try {
    Write-Host "Temporarily allowing local logins without a password..."
    (Get-Content $hba) -replace '^(host\s+all\s+all\s+(127\.0\.0\.1/32|::1/128)\s+)\S+', '${1}trust' |
        Set-Content $hba -Encoding ascii
    Restart-Service $Service

    Write-Host "Setting new password for user 'postgres'..."
    $escaped = $NewPassword.Replace("'", "''")
    & $psql -U postgres -h 127.0.0.1 -v ON_ERROR_STOP=1 -c "ALTER USER postgres PASSWORD '$escaped';"
    if ($LASTEXITCODE -ne 0) { throw "ALTER USER failed" }

    $exists = & $psql -U postgres -h 127.0.0.1 -tAc "SELECT 1 FROM pg_database WHERE datname = 'forms';"
    if ($exists -ne "1") {
        Write-Host "Creating database 'forms'..."
        & $psql -U postgres -h 127.0.0.1 -v ON_ERROR_STOP=1 -c "CREATE DATABASE forms;"
        if ($LASTEXITCODE -ne 0) { throw "CREATE DATABASE failed" }
    } else {
        Write-Host "Database 'forms' already exists."
    }
}
finally {
    Write-Host "Restoring original pg_hba.conf (password logins required again)..."
    Copy-Item $backup $hba -Force
    Restart-Service $Service
}

Write-Host "`nDone. Password for 'postgres' is now: $NewPassword" -ForegroundColor Green
