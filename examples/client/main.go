// Package main demonstrates using the taskqueue HTTP API with standard Go net/http.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const baseURL = "http://localhost:8080"

type SubmitRequest struct {
	Payload        any    `json:"payload"`
	Priority       int    `json:"priority,omitempty"`
	DelaySeconds   int    `json:"delay_seconds,omitempty"`
	MaxRetries     int    `json:"max_retries,omitempty"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

type SubmitResponse struct {
	ID        string    `json:"id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

type TaskResponse struct {
	ID          string     `json:"id"`
	State       string     `json:"state"`
	Payload     any        `json:"payload"`
	Priority    int        `json:"priority"`
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	LastError   *string    `json:"last_error"`
	CreatedAt   time.Time  `json:"created_at"`
	ScheduledAt *time.Time `json:"scheduled_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

func main() {
	client := &http.Client{Timeout: 5 * time.Second}

	// 1. Health Check
	fmt.Println("=== 1. Checking System Health ===")
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		log.Fatalf("Server unreachable at %s: %v", baseURL, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("Health response: %s\n\n", body)

	// 2. Submit an Immediate Task
	fmt.Println("=== 2. Submitting an Immediate Task ===")
	taskReq := SubmitRequest{
		Payload: map[string]string{
			"action":    "generate_report",
			"report_id": "rep_9812",
			"customer":  "acme_corp",
		},
		Priority:       2,
		MaxRetries:     3,
		TimeoutSeconds: 15,
		IdempotencyKey: fmt.Sprintf("report-%d", time.Now().UnixNano()),
	}

	reqBytes, _ := json.Marshal(taskReq)
	resp, err = client.Post(baseURL+"/tasks", "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		log.Fatalf("submit task failed: %v", err)
	}
	defer resp.Body.Close()

	var submitResp SubmitResponse
	if err := json.NewDecoder(resp.Body).Decode(&submitResp); err != nil {
		log.Fatalf("decode submit response: %v", err)
	}
	fmt.Printf("Task created: ID=%s State=%s\n\n", submitResp.ID, submitResp.State)

	// 3. Poll Task Status Until Completion
	fmt.Println("=== 3. Watching Task Lifecycle ===")
	for i := 0; i < 10; i++ {
		time.Sleep(100 * time.Millisecond)

		resp, err := client.Get(fmt.Sprintf("%s/tasks/%s", baseURL, submitResp.ID))
		if err != nil {
			log.Fatalf("get task failed: %v", err)
		}

		var task TaskResponse
		_ = json.NewDecoder(resp.Body).Decode(&task)
		resp.Body.Close()

		fmt.Printf("  Poll %d: state=%s\n", i+1, task.State)
		if task.State == "completed" || task.State == "failed" || task.State == "dead" {
			fmt.Printf("Task finished in terminal state: %s\n\n", task.State)
			break
		}
	}

	// 4. Submit a Scheduled Delayed Task
	fmt.Println("=== 4. Submitting a Delayed Task (delay: 2s) ===")
	delayedReq := SubmitRequest{
		Payload:      "delayed reminder message",
		DelaySeconds: 2,
	}
	dBytes, _ := json.Marshal(delayedReq)
	resp, err = client.Post(baseURL+"/tasks", "application/json", bytes.NewReader(dBytes))
	if err != nil {
		log.Fatalf("submit delayed task: %v", err)
	}
	defer resp.Body.Close()

	var dResp SubmitResponse
	_ = json.NewDecoder(resp.Body).Decode(&dResp)
	fmt.Printf("Delayed task enqueued: ID=%s State=%s (should be 'scheduled')\n", dResp.ID, dResp.State)

	// Read immediate status
	time.Sleep(50 * time.Millisecond)
	resp, _ = client.Get(baseURL + "/tasks/" + dResp.ID)
	var dTask TaskResponse
	_ = json.NewDecoder(resp.Body).Decode(&dTask)
	resp.Body.Close()
	fmt.Printf("Initial state: %s, scheduled_at=%v\n\n", dTask.State, dTask.ScheduledAt)

	// 5. Query Metrics
	fmt.Println("=== 5. Querying Prometheus Metrics ===")
	resp, err = client.Get(baseURL + "/metrics")
	if err == nil {
		defer resp.Body.Close()
		mBody, _ := io.ReadAll(resp.Body)
		fmt.Printf("Metrics snippet:\n%s\n", string(mBody[:min(len(mBody), 300)]))
	}

	fmt.Println("Client walkthrough completed successfully!")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
