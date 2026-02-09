// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Azure/kperf/api/types"
	"github.com/Azure/kperf/metrics"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

const defaultRequestTimeout = 60 * time.Second

// timeBucket groups requests that should execute in the same time window.
// Uses indices to avoid copying request data.
type timeBucket struct {
	timestamp int64  // bucket start time in ms
	startIdx  int    // start index in requests slice
	endIdx    int    // end index (exclusive) in requests slice
}

// workerMetrics holds per-worker statistics to avoid lock contention.
type workerMetrics struct {
	respMetric     metrics.ResponseMetric
	requestsRun    int
	requestsFailed int
}

// groupIntoTimeBuckets groups requests by time buckets to reduce timer overhead.
// Returns buckets with indices to avoid copying request data.
func groupIntoTimeBuckets(requests []types.ReplayRequest, bucketMs int64) []timeBucket {
	if len(requests) == 0 {
		return nil
	}

	buckets := make([]timeBucket, 0, len(requests)/100+1)
	currentBucket := timeBucket{
		timestamp: (requests[0].Timestamp / bucketMs) * bucketMs,
		startIdx:  0,
		endIdx:    0,
	}

	for i, req := range requests {
		bucketTime := (req.Timestamp / bucketMs) * bucketMs
		if bucketTime != currentBucket.timestamp {
			currentBucket.endIdx = i
			buckets = append(buckets, currentBucket)
			currentBucket = timeBucket{
				timestamp: bucketTime,
				startIdx:  i,
				endIdx:    i,
			}
		}
	}

	// Add final bucket
	currentBucket.endIdx = len(requests)
	buckets = append(buckets, currentBucket)

	klog.V(3).InfoS("Grouped requests into time buckets",
		"totalRequests", len(requests),
		"bucketCount", len(buckets),
		"bucketSizeMs", bucketMs)

	return buckets
}

// calculateBucketSize determines optimal bucket size based on request count and QPS.
func calculateBucketSize(requests []types.ReplayRequest) int64 {
	if len(requests) == 0 {
		return 10
	}

	duration := requests[len(requests)-1].Timestamp - requests[0].Timestamp
	if duration <= 0 {
		return 10
	}

	qps := float64(len(requests)) / (float64(duration) / 1000.0)

	// Adaptive bucket sizing:
	// - Low QPS (<100): 10ms buckets (good timing precision)
	// - Medium QPS (100-500): 20ms buckets
	// - High QPS (500-2000): 50ms buckets
	// - Very high QPS (>2000): 100ms buckets
	switch {
	case qps < 100:
		return 10
	case qps < 500:
		return 20
	case qps < 2000:
		return 50
	default:
		return 100
	}
}

// Runner executes replay requests at their scheduled timestamps.
type Runner struct {
	index       int
	requests    []types.ReplayRequest
	restClis    []rest.Interface // Changed from single to slice for round-robin
	baseURL     string
	workerCount int                              // Renamed from maxConcurrency to clarify semantics
	reqChan     chan *types.ReplayRequest       // Use pointer to avoid copying
}

// NewRunner creates a new replay runner.
func NewRunner(
	index int,
	requests []types.ReplayRequest,
	restClis []rest.Interface, // Changed from single to slice
	baseURL string,
	workerCount int,           // Renamed from maxConcurrency
) *Runner {
	// Default to 1 worker per connection if not specified
	if workerCount <= 0 {
		workerCount = len(restClis)
		if workerCount == 0 {
			workerCount = 1
		}
	}

	return &Runner{
		index:       index,
		requests:    requests,
		restClis:    restClis,
		baseURL:     baseURL,
		workerCount: workerCount,
	}
}

// RunnerResult contains the result of running replay requests.
type RunnerResult struct {
	Total          int
	Duration       time.Duration
	ResponseStats  types.ResponseStats
	RequestsRun    int
	RequestsFailed int
}

// Run executes all requests at their scheduled timestamps.
// replayStart is the time when the replay started (used for synchronization across runners).
func (r *Runner) Run(ctx context.Context, replayStart time.Time) (*RunnerResult, error) {
	if len(r.requests) == 0 {
		return &RunnerResult{}, nil
	}

	// Separate WATCH from normal requests using indices to avoid copying
	normalCount := 0
	watchCount := 0
	for i := range r.requests {
		if r.requests[i].Verb == "WATCH" {
			watchCount++
		} else {
			normalCount++
		}
	}

	// Pre-allocate with exact capacity
	normalReqs := make([]*types.ReplayRequest, 0, normalCount)
	watchReqs := make([]*types.ReplayRequest, 0, watchCount)

	for i := range r.requests {
		if r.requests[i].Verb == "WATCH" {
			watchReqs = append(watchReqs, &r.requests[i])
		} else {
			normalReqs = append(normalReqs, &r.requests[i])
		}
	}

	// Calculate optimal bucket size based on QPS
	var bucketMs int64
	if len(normalReqs) > 0 {
		// Create temporary slice view for bucket calculation
		tempReqs := make([]types.ReplayRequest, len(normalReqs))
		for i, req := range normalReqs {
			tempReqs[i] = *req
		}
		bucketMs = calculateBucketSize(tempReqs)

		duration := normalReqs[len(normalReqs)-1].Timestamp
		if duration > 0 {
			qps := float64(len(normalReqs)) / (float64(duration) / 1000.0)
			klog.V(2).InfoS("Runner configuration",
				"runner", r.index,
				"requests", len(normalReqs),
				"watches", len(watchReqs),
				"estimatedQPS", fmt.Sprintf("%.1f", qps),
				"bucketMs", bucketMs,
				"workers", r.workerCount,
				"connections", len(r.restClis))
		}
	}

	// Create buffered channel (10x worker count for high QPS) - using pointers
	r.reqChan = make(chan *types.ReplayRequest, r.workerCount*10)

	// Initialize per-worker metrics
	workers := make([]*workerMetrics, r.workerCount)
	for i := 0; i < r.workerCount; i++ {
		workers[i] = &workerMetrics{
			respMetric: metrics.NewResponseMetric(),
		}
	}

	// Start worker pool
	wg := r.startWorkers(ctx, workers)

	startTime := time.Now()

	// Scheduler goroutine: dispatch requests by direct slice iteration
	go func() {
		defer close(r.reqChan)

		for _, req := range normalReqs {
			// Wait for scheduled time
			scheduledTime := replayStart.Add(time.Duration(req.Timestamp) * time.Millisecond)
			waitDuration := time.Until(scheduledTime)

			if waitDuration > 0 {
				timer := time.NewTimer(waitDuration)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			// Dispatch request pointer (no copy)
			select {
			case <-ctx.Done():
				return
			case r.reqChan <- req:
				// Successfully dispatched
			}
		}
	}()

	// Handle WATCH requests separately (existing approach)
	var watchWg sync.WaitGroup
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()

	for _, req := range watchReqs {
		watchWg.Add(1)
		go func(req *types.ReplayRequest) {
			defer watchWg.Done()

			// Wait for scheduled time
			scheduledTime := replayStart.Add(time.Duration(req.Timestamp) * time.Millisecond)
			waitDuration := time.Until(scheduledTime)
			if waitDuration > 0 {
				timer := time.NewTimer(waitDuration)
				select {
				case <-watchCtx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}

			// Execute WATCH (use first worker's metrics for simplicity)
			_ = r.executeRequestWithClient(watchCtx, req, r.restClis[0], workers[0].respMetric)
		}(req)
	}

	// Wait for all normal workers to complete
	wg.Wait()

	// Cancel WATCH operations
	cancelWatch()

	// Aggregate results from all workers
	totalRun := 0
	totalFailed := 0
	aggregatedStats := types.ResponseStats{
		Errors:             make([]types.ResponseError, 0),
		LatenciesByURL:     make(map[string][]float64),
		TotalReceivedBytes: 0,
	}

	for _, wm := range workers {
		totalRun += wm.requestsRun
		totalFailed += wm.requestsFailed

		stats := wm.respMetric.Gather()
		aggregatedStats.Errors = append(aggregatedStats.Errors, stats.Errors...)

		for url, latencies := range stats.LatenciesByURL {
			if _, exists := aggregatedStats.LatenciesByURL[url]; !exists {
				aggregatedStats.LatenciesByURL[url] = make([]float64, 0, len(latencies))
			}
			aggregatedStats.LatenciesByURL[url] = append(aggregatedStats.LatenciesByURL[url], latencies...)
		}

		aggregatedStats.TotalReceivedBytes += stats.TotalReceivedBytes
	}

	return &RunnerResult{
		Total:          len(r.requests),
		Duration:       time.Since(startTime),
		ResponseStats:  aggregatedStats,
		RequestsRun:    totalRun,
		RequestsFailed: totalFailed,
	}, nil
}

// startWorkers creates the worker pool that processes requests from the channel.
func (r *Runner) startWorkers(ctx context.Context, workers []*workerMetrics) *sync.WaitGroup {
	var wg sync.WaitGroup

	for i := 0; i < r.workerCount; i++ {
		wg.Add(1)

		// Connection affinity: each worker uses same connection for cache locality
		connIndex := i % len(r.restClis)
		cli := r.restClis[connIndex]

		go func(workerID int, restCli rest.Interface, wm *workerMetrics) {
			defer wg.Done()

			for req := range r.reqChan {
				// Check context
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Execute request with worker's dedicated connection (pointer, no copy)
				err := r.executeRequestWithClient(ctx, req, restCli, wm.respMetric)

				// Track metrics without locking (per-worker metrics)
				wm.requestsRun++
				if err != nil {
					wm.requestsFailed++
				}
			}
		}(i, cli, workers[i])
	}

	return &wg
}

// executeRequestWithClient executes a single replay request with a specific client.
func (r *Runner) executeRequestWithClient(ctx context.Context, req *types.ReplayRequest, restCli rest.Interface, respMetric metrics.ResponseMetric) error {
	requester, err := NewReplayRequester(*req, restCli, r.baseURL)
	if err != nil {
		klog.V(5).Infof("Failed to create requester for %s %s: %v", req.Verb, req.APIPath, err)
		return err
	}

	requester.Timeout(defaultRequestTimeout)

	klog.V(5).Infof("Executing %s %s at timestamp %d", req.Verb, req.APIPath, req.Timestamp)

	start := time.Now()
	bytes, err := requester.Do(ctx)
	end := time.Now()
	latency := end.Sub(start).Seconds()

	respMetric.ObserveReceivedBytes(bytes)

	if err != nil {
		respMetric.ObserveFailure(requester.Method(), requester.MaskedURL().String(), end, latency, err)
		klog.V(5).Infof("Request failed: %s %s: %v", req.Verb, req.APIPath, err)
		return err
	}

	respMetric.ObserveLatency(requester.Method(), requester.MaskedURL().String(), latency)
	return nil
}

// executeRequest executes a single replay request (deprecated, kept for compatibility).
func (r *Runner) executeRequest(ctx context.Context, req *types.ReplayRequest, respMetric metrics.ResponseMetric) error {
	// Use first client for backward compatibility
	if len(r.restClis) == 0 {
		return fmt.Errorf("no REST clients available")
	}
	return r.executeRequestWithClient(ctx, req, r.restClis[0], respMetric)
}

// GetRunnerIndex returns the runner index from environment variable or parameter.
// In distributed mode, K8s injects JOB_COMPLETION_INDEX for indexed Jobs.
func GetRunnerIndex(paramIndex int) int {
	if idx := os.Getenv("JOB_COMPLETION_INDEX"); idx != "" {
		if i, err := strconv.Atoi(idx); err == nil {
			return i
		}
	}
	return paramIndex
}
