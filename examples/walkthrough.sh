#!/usr/bin/env bash
set -euo pipefail

BASE_URL="http://localhost:8080"

echo "============================================="
echo "  TaskQueue API — Walkthrough Script"
echo "============================================="

# 1. Health check
echo -e "\n[1] Health Check:"
curl -s "${BASE_URL}/health" | jq . || curl -s "${BASE_URL}/health"
echo ""

# 2. Submit a Task
echo -e "\n[2] Submitting a task with idempotency key:"
IDEM_KEY="demo-key-$(date +%s)"
SUBMIT_OUTPUT=$(curl -s -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d "{
    \"payload\": {\"action\": \"process_payment\", \"amount\": 49.99, \"currency\": \"USD\"},
    \"priority\": 2,
    \"timeout_seconds\": 10,
    \"max_retries\": 3,
    \"idempotency_key\": \"${IDEM_KEY}\"
  }")
echo "$SUBMIT_OUTPUT"

TASK_ID=$(echo "$SUBMIT_OUTPUT" | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo "Enqueued Task ID: ${TASK_ID}"

# 3. Test Duplicate Idempotency (Expect 409 Conflict)
echo -e "\n[3] Submitting duplicate idempotency key (expect 409 Conflict):"
curl -s -w "\nHTTP Status: %{http_code}\n" -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d "{
    \"payload\": \"duplicate\",
    \"idempotency_key\": \"${IDEM_KEY}\"
  }"

# 4. Check Task Status
echo -e "\n[4] Inspecting task state:"
curl -s "${BASE_URL}/tasks/${TASK_ID}"
echo ""

# 5. Submit a delayed task and cancel it
echo -e "\n[5] Submitting a scheduled delayed task (delay 30s):"
DELAYED_OUT=$(curl -s -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "payload": "cancel me before delay expires",
    "delay_seconds": 30
  }')
echo "$DELAYED_OUT"
DELAYED_ID=$(echo "$DELAYED_OUT" | grep -o '"id":"[^"]*' | cut -d'"' -f4)

echo -e "\n[6] Cancelling delayed task before execution:"
curl -s -X DELETE "${BASE_URL}/tasks/${DELAYED_ID}"
echo ""

# 7. Check DLQ
echo -e "\n[7] Querying Dead Letter Queue:"
curl -s "${BASE_URL}/dlq?limit=5&offset=0"
echo ""

# 8. Check Engine Stats
echo -e "\n[8] Querying Engine Runtime Stats:"
curl -s "${BASE_URL}/stats"
echo ""

echo -e "\nWalkthrough completed successfully!"
