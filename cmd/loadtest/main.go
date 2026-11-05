// Command loadtest fires POST /submissions at the API and reports how many
// submissions were stored, how many duplicates were rejected, errors, and
// throughput. With -dsn it also verifies the result against the database.
//
//	go run ./cmd/loadtest -total 100000 -dups 20000 -c 200 -dsn "$env:DATABASE_URL"
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"form-submission/internal/config"
)

type result struct {
	status  int // 0 = network/transport error
	latency time.Duration
}

func main() {
	var (
		baseURL     = flag.String("url", "http://localhost:8080", "API base URL")
		total       = flag.Int("total", 100_000, "total requests to send")
		dups        = flag.Int("dups", 20_000, "how many of the requests reuse an earlier idempotency_key")
		concurrency = flag.Int("c", 200, "concurrent workers")
		forms       = flag.Int("forms", 5, "spread submissions across this many form_ids")
		runID       = flag.String("run", fmt.Sprintf("run%d", time.Now().Unix()), "prefix for idempotency keys, so each run is independent")
		dsn         = flag.String("dsn", "", "Postgres DSN to verify stored rows against the database (default: DATABASE_URL from env or .env)")
	)
	flag.Parse()
	_ = config.LoadDotEnv(".env")
	if *dsn == "" {
		*dsn = os.Getenv("DATABASE_URL")
	}

	if *dups < 0 || *dups >= *total {
		fmt.Fprintln(os.Stderr, "-dups must be >= 0 and < -total")
		os.Exit(2)
	}
	unique := *total - *dups

	bodies := buildRequests(*runID, unique, *dups, *forms)
	fmt.Printf("Run %q: %d requests (%d unique keys + %d duplicates), %d workers, %d forms\n",
		*runID, *total, unique, *dups, *concurrency, *forms)

	results := fire(*baseURL+"/submissions", bodies, *concurrency)
	report(results, unique, *dups)

	if *dsn != "" {
		verify(*dsn, *runID, unique)
	}
}

// buildRequests creates `unique` distinct submissions plus `dups` extra
// requests that reuse already-generated keys (with identical payloads, as a
// retrying client would), then shuffles them so duplicates are interleaved
// with — and can race against — their originals.
func buildRequests(runID string, unique, dups, forms int) [][]byte {
	type req struct {
		FormID         string         `json:"form_id"`
		IdempotencyKey string         `json:"idempotency_key"`
		Email          string         `json:"email"`
		Data           map[string]any `json:"data"`
	}

	bodies := make([][]byte, 0, unique+dups)
	for i := range unique {
		b, _ := json.Marshal(req{
			FormID:         fmt.Sprintf("form-%d", i%forms),
			IdempotencyKey: fmt.Sprintf("%s-%d", runID, i),
			Email:          fmt.Sprintf("user%d@example.com", i),
			Data:           map[string]any{"name": fmt.Sprintf("User %d", i), "message": "hello"},
		})
		bodies = append(bodies, b)
	}
	// Duplicate keys are drawn from the unique set; if dups > unique, some
	// keys are reused more than once.
	for i := range dups {
		bodies = append(bodies, bodies[i%unique])
	}
	rand.Shuffle(len(bodies), func(i, j int) { bodies[i], bodies[j] = bodies[j], bodies[i] })
	return bodies
}

func fire(url string, bodies [][]byte, concurrency int) []result {
	// Keep-alive connections sized to the worker count; without this Go keeps
	// only 2 idle conns per host and Windows runs out of ephemeral ports.
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency,
			MaxIdleConnsPerHost: concurrency,
			MaxConnsPerHost:     concurrency,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	results := make([]result, len(bodies))
	var next, done atomic.Int64
	var wg sync.WaitGroup

	stopProgress := progress(&done, len(bodies))
	start := time.Now()

	for range concurrency {
		wg.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(bodies) {
					return
				}
				t := time.Now()
				status := 0
				resp, err := client.Post(url, "application/json", bytes.NewReader(bodies[i]))
				if err == nil {
					io.Copy(io.Discard, resp.Body) // drain so the conn is reused
					resp.Body.Close()
					status = resp.StatusCode
				}
				results[i] = result{status: status, latency: time.Since(t)}
				done.Add(1)
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	stopProgress()

	fmt.Printf("\nCompleted in %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput: %.0f requests/second\n", float64(len(bodies))/elapsed.Seconds())
	return results
}

func progress(done *atomic.Int64, total int) (stop func()) {
	ticker := time.NewTicker(time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Printf("\r  progress: %d / %d", done.Load(), total)
			case <-quit:
				return
			}
		}
	}()
	return func() { ticker.Stop(); close(quit) }
}

func report(results []result, unique, dups int) {
	var created, replayed, clientErr, serverErr, netErr int
	latencies := make([]time.Duration, 0, len(results))
	for _, r := range results {
		latencies = append(latencies, r.latency)
		switch {
		case r.status == http.StatusCreated:
			created++
		case r.status == http.StatusOK:
			replayed++
		case r.status == 0:
			netErr++
		case r.status >= 500:
			serverErr++
		default:
			clientErr++
		}
	}
	slices.Sort(latencies)
	pct := func(p float64) time.Duration { return latencies[int(float64(len(latencies)-1)*p)] }

	errors := clientErr + serverErr + netErr
	fmt.Println("\n================ LOAD TEST REPORT ================")
	fmt.Printf("Requests sent:                 %d\n", len(results))
	fmt.Printf("Stored (201 Created):          %d   (expected %d)\n", created, unique)
	fmt.Printf("Duplicates rejected (200):     %d   (expected %d)\n", replayed, dups)
	fmt.Printf("Errors:                        %d   (4xx=%d, 5xx=%d, network=%d)\n", errors, clientErr, serverErr, netErr)
	fmt.Printf("Latency p50 / p95 / p99:       %s / %s / %s\n",
		pct(0.50).Round(time.Microsecond), pct(0.95).Round(time.Microsecond), pct(0.99).Round(time.Microsecond))
	ok := created == unique && replayed == dups && errors == 0
	fmt.Printf("Result:                        %s\n", map[bool]string{true: "PASS", false: "FAIL"}[ok])
	fmt.Println("==================================================")
}

// verify checks the database directly, independent of what the API said.
func verify(dsn, runID string, unique int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Println("DB verify: connect failed:", err)
		return
	}
	defer conn.Close(ctx)

	var rows, distinct int
	err = conn.QueryRow(ctx, `
		SELECT count(*), count(DISTINCT idempotency_key)
		FROM submissions WHERE idempotency_key LIKE $1`, runID+"-%",
	).Scan(&rows, &distinct)
	if err != nil {
		fmt.Println("DB verify: query failed:", err)
		return
	}
	fmt.Println("\n================ DATABASE CHECK ==================")
	fmt.Printf("Rows stored for this run:      %d   (expected %d)\n", rows, unique)
	fmt.Printf("Distinct idempotency keys:     %d   (rows == distinct means no duplicates)\n", distinct)
	fmt.Printf("Result:                        %s\n", map[bool]string{true: "PASS", false: "FAIL"}[rows == unique && distinct == rows])
	fmt.Println("==================================================")
}
