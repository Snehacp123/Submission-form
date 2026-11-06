# Demonstrates Part 1 end to end against the running API.
# Usage: powershell -File scripts\smoke-test.ps1 [-Base http://localhost:8080]
param([string]$Base = "http://localhost:8080")
$ErrorActionPreference = "Stop"
$base = $Base
$tmp = New-TemporaryFile

function Call($label, $method, $path, $body) {
    Write-Host "`n=== $label ===" -ForegroundColor Cyan
    $curlArgs = @("-s", "-i", "-X", $method, "$base$path")
    if ($body) {
        [IO.File]::WriteAllText($tmp.FullName, $body)
        $curlArgs += @("-H", "Content-Type: application/json", "--data-binary", "@$tmp")
    }
    & curl.exe @curlArgs | Where-Object { $_ -notmatch '^(Date|Content-Length):' -and $_ -ne '' }
}

# Wait for the server to be up.
& curl.exe -s -o NUL --retry 30 --retry-connrefused --retry-delay 1 "$base/healthz"

Call "1. Valid submission -> 201 Created" POST "/submissions" `
    '{"form_id":"contact-form","idempotency_key":"abc-123","email":"user@example.com","data":{"name":"John","message":"Hello"}}'

Call "2. Same idempotency_key again -> 200 OK, same record" POST "/submissions" `
    '{"form_id":"contact-form","idempotency_key":"abc-123","email":"user@example.com","data":{"name":"John","message":"Hello"}}'

Call "3. Missing form_id -> 400" POST "/submissions" `
    '{"idempotency_key":"x1","email":"user@example.com"}'

Call "4. Invalid email -> 400" POST "/submissions" `
    '{"form_id":"contact-form","idempotency_key":"x2","email":"not-an-email"}'

# Seed 12 more so the form has 13 submissions (2 pages).
1..12 | ForEach-Object {
    [IO.File]::WriteAllText($tmp.FullName, "{`"form_id`":`"contact-form`",`"idempotency_key`":`"seed-$_`",`"email`":`"u$_@example.com`",`"data`":{`"n`":$_}}")
    & curl.exe -s -o NUL -X POST "$base/submissions" -H "Content-Type: application/json" --data-binary "@$tmp"
}

Call "5. List page 1 (10 per page)" GET "/forms/contact-form/submissions" $null
Call "6. List page 2 (remaining 3)" GET "/forms/contact-form/submissions?page=2" $null

Remove-Item $tmp
