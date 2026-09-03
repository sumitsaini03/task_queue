# PowerShell Walkthrough for TaskQueue API

$BaseURL = "http://localhost:8080"

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "  TaskQueue API -- PowerShell Walkthrough" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# 1. Health Check
Write-Host ""
Write-Host "[1] Health Check:" -ForegroundColor Yellow
$health = Invoke-RestMethod -Uri "$BaseURL/health" -Method Get
$health | ConvertTo-Json

# 2. Submit a Task
Write-Host ""
Write-Host "[2] Submitting a task with idempotency key:" -ForegroundColor Yellow
$idemKey = "order-" + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$body = @{
    payload = @{
        action = "send_welcome_email"
        email = "alice@example.com"
        tier = "premium"
    }
    priority = 1
    timeout_seconds = 10
    max_retries = 3
    idempotency_key = $idemKey
} | ConvertTo-Json

$submit = Invoke-RestMethod -Uri "$BaseURL/tasks" -Method Post -ContentType "application/json" -Body $body
$submit | ConvertTo-Json
$taskId = $submit.id

# 3. Duplicate Idempotency Key (Expect 409 Conflict)
Write-Host ""
Write-Host "[3] Submitting duplicate idempotency key (expect 409 Conflict):" -ForegroundColor Yellow
try {
    Invoke-RestMethod -Uri "$BaseURL/tasks" -Method Post -ContentType "application/json" -Body $body
} catch {
    Write-Host "Caught expected conflict response" -ForegroundColor Green
}

# 4. Inspect Task Status
Start-Sleep -Milliseconds 150
Write-Host ""
Write-Host "[4] Inspecting task state:" -ForegroundColor Yellow
$task = Invoke-RestMethod -Uri "$BaseURL/tasks/$taskId" -Method Get
$task | ConvertTo-Json

# 5. Submit Scheduled Delayed Task and Cancel it
Write-Host ""
Write-Host "[5] Submitting a scheduled delayed task (delay 30s):" -ForegroundColor Yellow
$delayedBody = @{
    payload = "delayed notification"
    delay_seconds = 30
} | ConvertTo-Json

$delayedSubmit = Invoke-RestMethod -Uri "$BaseURL/tasks" -Method Post -ContentType "application/json" -Body $delayedBody
$delayedSubmit | ConvertTo-Json
$delayedId = $delayedSubmit.id

Write-Host ""
Write-Host "[6] Cancelling delayed task before promotion:" -ForegroundColor Yellow
$cancelResp = Invoke-RestMethod -Uri "$BaseURL/tasks/$delayedId" -Method Delete
$cancelResp | ConvertTo-Json

# 6. DLQ Query
Write-Host ""
Write-Host "[7] Querying Dead Letter Queue:" -ForegroundColor Yellow
$dlq = Invoke-RestMethod -Uri "$BaseURL/dlq" -Method Get
$dlq | ConvertTo-Json

# 7. Engine Stats
Write-Host ""
Write-Host "[8] Querying Engine Runtime Stats:" -ForegroundColor Yellow
$stats = Invoke-RestMethod -Uri "$BaseURL/stats" -Method Get
$stats | ConvertTo-Json

Write-Host ""
Write-Host "Walkthrough completed successfully!" -ForegroundColor Green
