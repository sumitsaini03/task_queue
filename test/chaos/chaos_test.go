package chaos

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestChaos_KillProcessMidWriteAndRecover(t *testing.T) {
	// 1. Build real server binary
	tempDir := t.TempDir()
	walDir := filepath.Join(tempDir, "wal_data")
	binName := "server_bin"
	if runtime.GOOS == "windows" {
		binName += ".exe"
	}
	binPath := filepath.Join(tempDir, binName)

	buildCmd := exec.Command("go", "build", "-o", binPath, "../../cmd/server")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build server binary failed: %v\nOutput: %s", err, string(out))
	}

	// 2. Start server process
	serverCmd := exec.Command(binPath)
	serverCmd.Env = append(os.Environ(),
		"WAL_DIR="+walDir,
		"HTTP_ADDR=127.0.0.1:8089",
		"WAL_SYNC_POLICY=none",
	)
	if err := serverCmd.Start(); err != nil {
		t.Fatalf("start server failed: %v", err)
	}

	// Wait for health check
	baseURL := "http://127.0.0.1:8089"
	client := &http.Client{Timeout: 500 * time.Millisecond}
	healthy := false
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := client.Get(baseURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			healthy = true
			break
		}
	}
	if !healthy {
		_ = serverCmd.Process.Kill()
		t.Fatalf("server did not become healthy in time")
	}

	// 3. Hammer server with concurrent writes
	var (
		wg             sync.WaitGroup
		acknowledgedMu sync.Mutex
		acknowledged   = make([]string, 0, 500)
		stopProducer   atomic.Bool
	)

	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			c := &http.Client{Timeout: 1 * time.Second}
			for i := 0; !stopProducer.Load(); i++ {
				body := fmt.Sprintf(`{"payload":{"worker":%d,"iter":%d,"data":"chaos payload"},"priority":1}`, workerID, i)
				resp, err := c.Post(baseURL+"/tasks", "application/json", bytes.NewBufferString(body))
				if err != nil {
					continue
				}
				if resp.StatusCode == http.StatusCreated {
					var res struct {
						ID string `json:"id"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&res); err == nil {
						acknowledgedMu.Lock()
						acknowledged = append(acknowledged, res.ID)
						acknowledgedMu.Unlock()
					}
				}
				resp.Body.Close()
			}
		}(p)
	}

	// Wait for at least 50 tasks to be acknowledged
	for i := 0; i < 50; i++ {
		time.Sleep(20 * time.Millisecond)
		acknowledgedMu.Lock()
		count := len(acknowledged)
		acknowledgedMu.Unlock()
		if count >= 50 {
			break
		}
	}

	// 4. Force KILL process mid-write (SIGKILL)
	if err := serverCmd.Process.Kill(); err != nil {
		t.Fatalf("kill server failed: %v", err)
	}
	_ = serverCmd.Wait() // Reaps the dead process
	stopProducer.Store(true)
	wg.Wait()

	acknowledgedMu.Lock()
	ackedCount := len(acknowledged)
	sampleIDs := make([]string, min(len(acknowledged), 10))
	copy(sampleIDs, acknowledged[:len(sampleIDs)])
	acknowledgedMu.Unlock()

	t.Logf("Process killed violently. Acknowledged before kill: %d tasks", ackedCount)
	if ackedCount == 0 {
		t.Fatalf("expected at least some tasks acknowledged before kill")
	}

	// 5. Restart server from the same WAL directory
	restartCmd := exec.Command(binPath)
	restartCmd.Env = append(os.Environ(),
		"WAL_DIR="+walDir,
		"HTTP_ADDR=127.0.0.1:8089",
		"WAL_SYNC_POLICY=none",
	)
	if err := restartCmd.Start(); err != nil {
		t.Fatalf("restart server failed: %v", err)
	}
	defer func() {
		_ = restartCmd.Process.Kill()
	}()

	// Wait for recovery and health check
	healthy = false
	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := client.Get(baseURL + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			healthy = true
			break
		}
	}
	if !healthy {
		t.Fatalf("restarted server failed to become healthy")
	}

	// 6. Assert recovered state: check that acknowledged tasks are recovered
	recoveredFound := 0
	for _, id := range sampleIDs {
		resp, err := client.Get(baseURL + "/tasks/" + id)
		if err == nil && resp.StatusCode == http.StatusOK {
			var task struct {
				ID    string `json:"id"`
				State string `json:"state"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&task); err == nil {
				if task.ID == id {
					recoveredFound++
				}
			}
			resp.Body.Close()
		}
	}

	t.Logf("Sample checked: %d/%d tasks confirmed restored in state registry", recoveredFound, len(sampleIDs))
	if recoveredFound == 0 {
		t.Fatalf("expected recovered tasks to be found in state registry")
	}

	// 7. Verify server can accept and process new tasks after recovery
	newResp, err := client.Post(baseURL+"/tasks", "application/json", bytes.NewBufferString(`{"payload":"post-chaos test"}`))
	if err != nil || newResp.StatusCode != http.StatusCreated {
		t.Fatalf("failed to submit task after chaos restart: %v", err)
	}
	newResp.Body.Close()

	t.Log("Chaos test passed: mid-write kill survived, state recovered, new traffic accepted seamlessly")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
