// Package main is an HTTP load testing tool for the taskqueue API.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Result struct {
	Duration time.Duration
	Status   int
	Err      error
}

func main() {
	targetURL := flag.String("url", "http://localhost:8080/tasks", "Target HTTP API URL")
	concurrency := flag.Int("c", 32, "Concurrent worker routines")
	totalRequests := flag.Int("n", 5000, "Total requests to send")
	timeout := flag.Duration("t", 5*time.Second, "Request timeout")
	flag.Parse()

	fmt.Println("=========================================================")
	fmt.Println("       TaskQueue HTTP API Saturation Load Test           ")
	fmt.Println("=========================================================")
	fmt.Printf("Target URL:       %s\n", *targetURL)
	fmt.Printf("Concurrency:      %d workers\n", *concurrency)
	fmt.Printf("Total Requests:   %d\n", *totalRequests)
	fmt.Println("---------------------------------------------------------")

	transport := &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 500,
		IdleConnTimeout:     30 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   *timeout,
	}

	reqPayload := []byte(`{"payload":{"action":"benchmark_task","data":"performance_testing_payload"},"priority":1}`)

	reqPerWorker := *totalRequests / *concurrency
	var (
		wg        sync.WaitGroup
		successes atomic.Int64
		conflicts atomic.Int64
		throttled atomic.Int64
		failures  atomic.Int64
		latencies = make([]time.Duration, 0, *totalRequests)
		latMu     sync.Mutex
	)

	startTime := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localLatencies := make([]time.Duration, 0, reqPerWorker)

			for i := 0; i < reqPerWorker; i++ {
				reqStart := time.Now()
				req, err := http.NewRequest(http.MethodPost, *targetURL, bytes.NewReader(reqPayload))
				if err != nil {
					failures.Add(1)
					continue
				}
				req.Header.Set("Content-Type", "application/json")

				resp, err := client.Do(req)
				duration := time.Since(reqStart)
				localLatencies = append(localLatencies, duration)

				if err != nil {
					failures.Add(1)
					continue
				}

				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusCreated, http.StatusOK:
					successes.Add(1)
				case http.StatusConflict:
					conflicts.Add(1)
				case http.StatusTooManyRequests:
					throttled.Add(1)
				default:
					failures.Add(1)
				}
			}

			latMu.Lock()
			latencies = append(latencies, localLatencies...)
			latMu.Unlock()
		}(w)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Calculate statistics
	totalCompleted := successes.Load() + conflicts.Load() + throttled.Load() + failures.Load()
	rps := float64(totalCompleted) / totalDuration.Seconds()

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	var p50, p90, p99, minLat, maxLat time.Duration
	if len(latencies) > 0 {
		minLat = latencies[0]
		maxLat = latencies[len(latencies)-1]
		p50 = latencies[int(float64(len(latencies))*0.50)]
		p90 = latencies[int(float64(len(latencies))*0.90)]
		p99 = latencies[int(float64(len(latencies))*0.99)]
	}

	fmt.Println("\n================= Results Summary =================")
	fmt.Printf("Elapsed Time:          %v\n", totalDuration.Round(time.Millisecond))
	fmt.Printf("Total Requests:        %d\n", totalCompleted)
	fmt.Printf("Successful (201/200):  %d\n", successes.Load())
	fmt.Printf("Rate Limited (429):    %d\n", throttled.Load())
	fmt.Printf("Conflicts (409):       %d\n", conflicts.Load())
	fmt.Printf("Failures / Errors:     %d\n", failures.Load())
	fmt.Printf("Sustained Throughput:  %.1f req/sec\n", rps)
	fmt.Println("----------------- Latency Distribution -----------------")
	fmt.Printf("Min Latency:           %v\n", minLat.Round(time.Microsecond))
	fmt.Printf("p50 (Median):          %v\n", p50.Round(time.Microsecond))
	fmt.Printf("p90:                   %v\n", p90.Round(time.Microsecond))
	fmt.Printf("p99:                   %v\n", p99.Round(time.Microsecond))
	fmt.Printf("Max Latency:           %v\n", maxLat.Round(time.Microsecond))
	fmt.Println("========================================================")
}
