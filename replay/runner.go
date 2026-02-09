// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"context"
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

// Runner executes replay requests at their scheduled timestamps.
type Runner struct {
	index          int
	requests       []types.ReplayRequest
	restCli        rest.Interface
	baseURL        string
	maxConcurrency int
}

// NewRunner creates a new replay runner.
func NewRunner(
	index int,
	requests []types.ReplayRequest,
	restCli rest.Interface,
	baseURL string,
	maxConcurrency int,
) *Runner {
	// Default to unlimited concurrency
	if maxConcurrency <= 0 {
		maxConcurrency = len(requests)
		if maxConcurrency == 0 {
			maxConcurrency = 1
		}
	}

	return &Runner{
		index:          index,
		requests:       requests,
		restCli:        restCli,
		baseURL:        baseURL,
		maxConcurrency: maxConcurrency,
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

	respMetric := metrics.NewResponseMetric()

	var wg sync.WaitGroup
	var watchWg sync.WaitGroup // Separate wait group for WATCH operations
	sem := make(chan struct{}, r.maxConcurrency)

	startTime := time.Now()
	requestsRun := 0
	requestsFailed := 0
	var mu sync.Mutex

	// Create a cancellable context for WATCH operations
	watchCtx, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch() // Ensure WATCH operations are cancelled when we're done

	for _, req := range r.requests {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			break
		default:
		}

		// Wait until scheduled time
		scheduledTime := replayStart.Add(time.Duration(req.Timestamp) * time.Millisecond)
		waitDuration := time.Until(scheduledTime)
		if waitDuration > 0 {
			timer := time.NewTimer(waitDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				break
			case <-timer.C:
			}
		}

		// WATCH operations use separate wait group and context
		isWatch := req.Verb == "WATCH"
		if isWatch {
			watchWg.Add(1)
		} else {
			wg.Add(1)
		}

		// Spawn goroutine immediately without blocking on semaphore
		// The goroutine will acquire the semaphore when it starts
		go func(req types.ReplayRequest, isWatch bool) {
			if isWatch {
				defer watchWg.Done()
			} else {
				defer wg.Done()
			}

			// Acquire semaphore slot inside goroutine (non-blocking main loop)
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()

			// Use watchCtx for WATCH operations so they can be cancelled
			execCtx := ctx
			if isWatch {
				execCtx = watchCtx
			}

			err := r.executeRequest(execCtx, req, respMetric)

			mu.Lock()
			requestsRun++
			if err != nil {
				requestsFailed++
			}
			mu.Unlock()
		}(req, isWatch)
	}

	// Wait only for non-WATCH requests to complete
	wg.Wait()

	// Cancel all WATCH operations (they'll be cleaned up in background)
	cancelWatch()

	// Don't wait for WATCH operations - let them terminate via context cancellation
	// The watchWg cleanup happens asynchronously

	return &RunnerResult{
		Total:          len(r.requests),
		Duration:       time.Since(startTime),
		ResponseStats:  respMetric.Gather(),
		RequestsRun:    requestsRun,
		RequestsFailed: requestsFailed,
	}, nil
}

// executeRequest executes a single replay request.
func (r *Runner) executeRequest(ctx context.Context, req types.ReplayRequest, respMetric metrics.ResponseMetric) error {
	requester, err := NewReplayRequester(req, r.restCli, r.baseURL)
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
