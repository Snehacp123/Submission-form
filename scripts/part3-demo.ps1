# Part 3 demo: shows Flink's per-form, per-hour counts updating live.
# Needs the Docker stack running and the Flink job submitted (see README).
# Usage: powershell -File scripts\part3-demo.ps1 [-Base http://localhost:8080]
param([string]$Base = "http://localhost:8080")
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot)

function Show-Counts($title) {
    Write-Host "`n=== $title ===" -ForegroundColor Cyan
    docker compose exec -T postgres psql -U postgres -d forms -c `
        "SELECT form_id, hour_start AT TIME ZONE 'UTC' AS hour_start_utc, submission_count FROM form_hourly_counts WHERE form_id LIKE 'demo-%' ORDER BY form_id, hour_start;"
}

$run = Get-Date -Format "HHmmss"

# The API always stamps created_at = now(), so to show more than one hourly
# bucket we insert a few rows with earlier timestamps straight into Postgres.
# Flink sees them through CDC exactly like API inserts.
Write-Host "Seeding rows 1 and 2 hours ago (directly in Postgres)..." -ForegroundColor Yellow
docker compose exec -T postgres psql -q -U postgres -d forms -c @"
INSERT INTO submissions (form_id, idempotency_key, email, created_at)
SELECT 'demo-contact', 'demo-$run-past-' || g, 'past' || g || '@example.com',
       now() - (CASE WHEN g <= 3 THEN interval '2 hours' ELSE interval '1 hour' END)
FROM generate_series(1, 7) g;
"@
Start-Sleep 3
Show-Counts "Before live submissions"

for ($round = 1; $round -le 3; $round++) {
    Write-Host "`nRound ${round}: posting 5 submissions to demo-contact and 2 to demo-newsletter via the API" -ForegroundColor Yellow
    1..7 | ForEach-Object {
        $form = if ($_ -le 5) { "demo-contact" } else { "demo-newsletter" }
        $body = @{ form_id = $form; idempotency_key = "demo-$run-$round-$_"; email = "user$_@example.com"; data = @{ round = $round } } | ConvertTo-Json -Compress
        Invoke-RestMethod "$Base/submissions" -Method Post -ContentType "application/json" -Body $body | Out-Null
    }
    # Re-send one key: a duplicate must not change any count.
    $dup = @{ form_id = "demo-contact"; idempotency_key = "demo-$run-$round-1"; email = "user1@example.com" } | ConvertTo-Json -Compress
    Invoke-RestMethod "$Base/submissions" -Method Post -ContentType "application/json" -Body $dup | Out-Null
    Start-Sleep 3
    Show-Counts "After round $round (the current hour grows by 5 and 2)"
}

Write-Host "`n=== Flink counts vs. counting submissions directly ===" -ForegroundColor Cyan
Get-Content scripts\verify-counts.sql -Raw | docker compose exec -T postgres psql -U postgres -d forms
Write-Host "Also visible at $Base (Live hourly counts section) and $Base/forms/demo-contact/hourly-counts"
