# Part 3 crash test: kills the Flink TaskManager while submissions are flowing,
# restarts it, and checks Flink's counts still match the database exactly
# (nothing lost, nothing counted twice).
# Usage: powershell -File scripts\part3-crash-test.ps1 [-Base http://localhost:8080] [-Total 20000]
param([string]$Base = "http://localhost:8080", [int]$Total = 20000, [int]$PgPort = 5435)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot)
$flink = "http://localhost:$(if ($env:FLINK_UI_PORT) { $env:FLINK_UI_PORT } else { 8082 })"

function Get-FlinkJob { (Invoke-RestMethod "$flink/jobs/overview").jobs | Where-Object { $_.name -eq "form-hourly-submission-counts" -and $_.state -notin "CANCELED","FAILED","FINISHED" } | Select-Object -First 1 }
function Get-Restores($jid) { (Invoke-RestMethod "$flink/jobs/$jid/checkpoints") }

$job = Get-FlinkJob
if (-not $job) { throw "Flink job is not running. Submit it first: docker compose run --rm flink-job" }
$restoresBefore = [int](Get-Restores $job.jid).counts.restored
Write-Host "Flink job $($job.jid) is $($job.state), checkpoint restores so far: $restoresBefore" -ForegroundColor Cyan

# 1. Start load in the background (20% duplicates), so the crash happens mid-stream.
$dups = [int]($Total / 5)
Write-Host "`n1. Sending $Total requests ($dups duplicates) in the background..." -ForegroundColor Yellow
$dsn = "postgres://postgres:postgres@localhost:$PgPort/forms?sslmode=disable"
$load = Start-Job -ArgumentList (Get-Location).Path, $Base, $Total, $dups, $dsn -ScriptBlock {
    param($dir, $base, $total, $dups, $dsn)
    Set-Location $dir
    go run ./cmd/loadtest -url $base -total $total -dups $dups -c 20 -dsn $dsn 2>&1
}

# 2. Crash the TaskManager (SIGKILL: no clean shutdown, in-memory state is gone)
#    once the load is well under way.
function Get-RowCount { [int](docker compose exec -T postgres psql -U postgres -d forms -t -A -c "SELECT count(*) FROM submissions") }
$startRows = Get-RowCount
do { Start-Sleep 1 } while ((Get-RowCount) -lt $startRows + [int]($Total / 5) -and $load.State -eq "Running")
Write-Host "2. Killing the Flink TaskManager mid-stream..." -ForegroundColor Red
docker compose kill taskmanager | Out-Null
Start-Sleep 10
Write-Host "3. Starting a fresh TaskManager..." -ForegroundColor Yellow
docker compose start taskmanager | Out-Null

# 3. Wait for the load to finish.
Wait-Job $load | Out-Null
Write-Host "`n4. Load test output:" -ForegroundColor Cyan
Receive-Job $load | Where-Object { $_ -notmatch 'progress:' } | ForEach-Object { "   $_" }
Remove-Job $load

# 4. Wait for the job to be RUNNING again and for the counts to catch up.
Write-Host "`n5. Waiting for Flink to recover and catch up..." -ForegroundColor Yellow
$check = Get-Content scripts\verify-counts.sql -Raw
$deadline = (Get-Date).AddMinutes(3)
do {
    Start-Sleep 3
    $j = Get-FlinkJob
    $out = $check | docker compose exec -T postgres psql -U postgres -d forms -t -A -F ","
    $cols = ($out | Where-Object { $_ -match '^\d' } | Select-Object -First 1) -split ","
    Write-Host "   job=$($j.state) rows=$($cols[0]) flink_total=$($cols[1]) mismatched_groups=$($cols[3])"
    $cp = Get-Restores $j.jid
} while (($j.state -ne "RUNNING" -or $cols[3] -ne "0" -or [int]$cp.counts.restored -le $restoresBefore) -and (Get-Date) -lt $deadline)

$restored = [int]$cp.counts.restored - $restoresBefore
Write-Host "`n================ CRASH TEST RESULT ================" -ForegroundColor Cyan
Write-Host "Recoveries during test:     $restored (restored from checkpoint #$($cp.latest.restored.id))"
Write-Host "Rows in submissions:        $($cols[0])"
Write-Host "Total counted by Flink:     $($cols[1])"
Write-Host "Mismatched (form, hour):    $($cols[3])"
$pass = $cols[3] -eq "0" -and $cols[0] -eq $cols[1] -and $restored -ge 1
Write-Host "Result:                     $(if ($pass) { 'PASS - nothing lost, nothing double-counted' } else { 'FAIL' })" -ForegroundColor $(if ($pass) { 'Green' } else { 'Red' })
Write-Host "==================================================="
