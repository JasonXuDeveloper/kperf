// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/kperf/api/types"
	"github.com/Azure/kperf/request"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

// ScheduleResult contains the aggregated result from all runners.
type ScheduleResult struct {
	// Per-runner results
	RunnerResults []*RunnerResult
	// Total duration of the entire replay
	Duration time.Duration
	// Aggregated stats
	Aggregated types.ResponseStats
	// Total requests across all runners
	TotalRequests int
	// Total requests run
	TotalRun int
	// Total requests failed
	TotalFailed int
}

// Schedule orchestrates replay execution across multiple local runners.
func Schedule(ctx context.Context, kubeconfigPath string, profile *types.ReplayProfile) (*ScheduleResult, error) {
	// Build REST config
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	runnerCount := profile.Spec.RunnerCount
	klog.V(2).InfoS("Starting local replay",
		"runnerCount", runnerCount,
		"totalRequests", len(profile.Requests),
		"duration", fmt.Sprintf("%dms", profile.Duration()),
	)

	// Create REST clients using the same method as existing kperf
	// This handles auth, TLS, content negotiation properly
	restClis, err := request.NewClients(kubeconfigPath,
		profile.Spec.ConnsPerRunner,
		request.WithClientContentTypeOpt(profile.Spec.ContentType),
		request.WithClientDisableHTTP2Opt(profile.Spec.DisableHTTP2),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST clients: %w", err)
	}

	// Partition requests across runners
	runnerRequests := make([][]types.ReplayRequest, runnerCount)
	for i := 0; i < runnerCount; i++ {
		runnerRequests[i] = PartitionRequests(profile.Requests, runnerCount, i)
		klog.V(3).InfoS("Partitioned requests for runner",
			"runner", i,
			"requests", len(runnerRequests[i]),
		)
	}

	// Create runners (reuse clients across runners)
	runners := make([]*Runner, runnerCount)
	for i := 0; i < runnerCount; i++ {
		// Round-robin client assignment
		cli := restClis[i%len(restClis)]
		runners[i] = NewRunner(
			i,
			runnerRequests[i],
			cli,
			restConfig.Host,
			profile.Spec.ClientsPerRunner,
		)
	}

	// Synchronize start time across all runners
	replayStart := time.Now()
	startTime := replayStart

	// Run all runners concurrently
	var wg sync.WaitGroup
	results := make([]*RunnerResult, runnerCount)
	errors := make([]error, runnerCount)

	for i := 0; i < runnerCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			result, err := runners[idx].Run(ctx, replayStart)
			results[idx] = result
			errors[idx] = err
		}(i)
	}

	wg.Wait()
	totalDuration := time.Since(startTime)

	// Check for errors
	for i, err := range errors {
		if err != nil {
			klog.V(2).ErrorS(err, "Runner failed", "runner", i)
		}
	}

	// Aggregate results
	return aggregateResults(results, totalDuration), nil
}

// ScheduleSingleRunner runs a single runner (for distributed mode).
func ScheduleSingleRunner(ctx context.Context, kubeconfigPath string, profile *types.ReplayProfile, runnerIndex int) (*RunnerResult, error) {
	// Build REST config
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	// Create REST clients using the same method as existing kperf
	restClis, err := request.NewClients(kubeconfigPath,
		profile.Spec.ConnsPerRunner,
		request.WithClientContentTypeOpt(profile.Spec.ContentType),
		request.WithClientDisableHTTP2Opt(profile.Spec.DisableHTTP2),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST clients: %w", err)
	}

	// Partition requests for this runner
	requests := PartitionRequests(profile.Requests, profile.Spec.RunnerCount, runnerIndex)

	klog.V(2).InfoS("Starting single runner",
		"runnerIndex", runnerIndex,
		"runnerCount", profile.Spec.RunnerCount,
		"requests", len(requests),
	)

	// Use first client (all share same connection pool behavior)
	runner := NewRunner(
		runnerIndex,
		requests,
		restClis[0],
		restConfig.Host,
		profile.Spec.ClientsPerRunner,
	)

	// Use current time as start (each pod will have slightly different start times)
	replayStart := time.Now()

	return runner.Run(ctx, replayStart)
}

// aggregateResults combines results from all runners.
func aggregateResults(results []*RunnerResult, totalDuration time.Duration) *ScheduleResult {
	schedResult := &ScheduleResult{
		RunnerResults: results,
		Duration:      totalDuration,
		Aggregated: types.ResponseStats{
			Errors:             make([]types.ResponseError, 0),
			LatenciesByURL:     make(map[string][]float64),
			TotalReceivedBytes: 0,
		},
	}

	for _, result := range results {
		if result == nil {
			continue
		}

		schedResult.TotalRequests += result.Total
		schedResult.TotalRun += result.RequestsRun
		schedResult.TotalFailed += result.RequestsFailed

		// Aggregate errors
		schedResult.Aggregated.Errors = append(schedResult.Aggregated.Errors, result.ResponseStats.Errors...)

		// Aggregate latencies by URL
		for url, latencies := range result.ResponseStats.LatenciesByURL {
			if _, exists := schedResult.Aggregated.LatenciesByURL[url]; !exists {
				schedResult.Aggregated.LatenciesByURL[url] = make([]float64, 0)
			}
			schedResult.Aggregated.LatenciesByURL[url] = append(schedResult.Aggregated.LatenciesByURL[url], latencies...)
		}

		// Sum bytes
		schedResult.Aggregated.TotalReceivedBytes += result.ResponseStats.TotalReceivedBytes
	}

	return schedResult
}

// NewClientsForReplay creates REST clients for replay execution.
// This is a convenience wrapper around request.NewClients.
func NewClientsForReplay(kubeconfigPath string, conns int, contentType types.ContentType, disableHTTP2 bool) ([]rest.Interface, error) {
	return request.NewClients(kubeconfigPath,
		conns,
		request.WithClientContentTypeOpt(contentType),
		request.WithClientDisableHTTP2Opt(disableHTTP2),
	)
}
