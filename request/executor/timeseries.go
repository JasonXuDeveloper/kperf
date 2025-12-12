// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/kperf/api/types"
)

// TimeSeriesExecutor implements Executor for time-series replay mode.
// It dispatches requests according to recorded timestamps from audit logs.
type TimeSeriesExecutor struct {
	config       *types.TimeSeriesConfig
	spec         *types.LoadProfileSpec
	interval     time.Duration
	buckets      []types.RequestBucket
	reqBuilderCh chan RESTRequestBuilder
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	once         sync.Once
}

// NewTimeSeriesExecutor creates a new time series executor from spec.
func NewTimeSeriesExecutor(spec *types.LoadProfileSpec) (Executor, error) {
	if spec.Mode != types.ModeTimeSeries {
		return nil, fmt.Errorf("expected mode %s, got %s", types.ModeTimeSeries, spec.Mode)
	}

	if spec.ModeConfig == nil {
		return nil, fmt.Errorf("modeConfig is required")
	}

	// Type assert to TimeSeriesConfig
	config, ok := spec.ModeConfig.(*types.TimeSeriesConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type for time-series mode")
	}

	interval, err := time.ParseDuration(config.Interval)
	if err != nil {
		return nil, fmt.Errorf("invalid interval: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &TimeSeriesExecutor{
		config:       config,
		spec:         spec,
		interval:     interval,
		buckets:      config.Buckets,
		reqBuilderCh: make(chan RESTRequestBuilder, 1000),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Chan returns the channel that produces request builders.
func (e *TimeSeriesExecutor) Chan() <-chan RESTRequestBuilder {
	return e.reqBuilderCh
}

// Run starts the executor and begins replaying requests.
func (e *TimeSeriesExecutor) Run(ctx context.Context) error {
	defer e.wg.Done()
	e.wg.Add(1)

	startTime := time.Now()

	for _, bucket := range e.buckets {
		targetTime := startTime.Add(time.Duration(bucket.StartTime * float64(time.Second)))

		// Wait until target time
		select {
		case <-time.After(time.Until(targetTime)):
		case <-ctx.Done():
			return ctx.Err()
		case <-e.ctx.Done():
			return e.ctx.Err()
		}

		// Dispatch requests in this bucket
		for _, req := range bucket.Requests {
			builder := e.createBuilderForExactRequest(&req)
			if builder == nil {
				continue
			}
			select {
			case e.reqBuilderCh <- builder:
			case <-ctx.Done():
				return ctx.Err()
			case <-e.ctx.Done():
				return e.ctx.Err()
			}
		}
	}

	return nil
}

// Stop gracefully stops the executor.
func (e *TimeSeriesExecutor) Stop() {
	e.once.Do(func() {
		e.cancel()
		e.wg.Wait()
		close(e.reqBuilderCh)
	})
}

// Metadata returns executor metadata.
func (e *TimeSeriesExecutor) Metadata() ExecutorMetadata {
	totalRequests := 0
	for _, bucket := range e.buckets {
		totalRequests += len(bucket.Requests)
	}

	maxDuration := 0.0
	if len(e.buckets) > 0 {
		maxDuration = e.buckets[len(e.buckets)-1].StartTime
	}

	return ExecutorMetadata{
		ExpectedTotal:    totalRequests,
		ExpectedDuration: time.Duration(maxDuration * float64(time.Second)),
		Custom: map[string]interface{}{
			"mode":         string(types.ModeTimeSeries),
			"bucket_count": len(e.buckets),
			"interval":     e.interval.String(),
		},
	}
}

// createBuilderForExactRequest creates a request builder from an ExactRequest.
// This function needs access to the request builder factory.
func (e *TimeSeriesExecutor) createBuilderForExactRequest(req *types.ExactRequest) RESTRequestBuilder {
	if createRequestBuilderFunc == nil {
		return nil
	}

	// Convert ExactRequest to WeightedRequest for the factory
	weightedReq := &types.WeightedRequest{
		Shares: 1,
	}

	// Map ExactRequest to appropriate WeightedRequest field based on method
	switch req.Method {
	case "GET":
		weightedReq.QuorumGet = &types.RequestGet{
			KubeGroupVersionResource: types.KubeGroupVersionResource{
				Group:    req.Group,
				Version:  req.Version,
				Resource: req.Resource,
			},
			Namespace: req.Namespace,
			Name:      req.Name,
		}
	case "LIST":
		weightedReq.QuorumList = &types.RequestList{
			KubeGroupVersionResource: types.KubeGroupVersionResource{
				Group:    req.Group,
				Version:  req.Version,
				Resource: req.Resource,
			},
			Namespace:     req.Namespace,
			Selector:      req.LabelSelector,
			FieldSelector: req.FieldSelector,
		}
	case "PATCH":
		patchType, _ := types.GetPatchType(req.PatchType)
		weightedReq.Patch = &types.RequestPatch{
			KubeGroupVersionResource: types.KubeGroupVersionResource{
				Group:    req.Group,
				Version:  req.Version,
				Resource: req.Resource,
			},
			Namespace: req.Namespace,
			Name:      req.Name,
			Body:      req.Body,
			PatchType: string(patchType),
		}
	case "POST":
		weightedReq.PostDel = &types.RequestPostDel{
			KubeGroupVersionResource: types.KubeGroupVersionResource{
				Group:    req.Group,
				Version:  req.Version,
				Resource: req.Resource,
			},
			Namespace: req.Namespace,
		}
	case "DELETE":
		weightedReq.PostDel = &types.RequestPostDel{
			KubeGroupVersionResource: types.KubeGroupVersionResource{
				Group:    req.Group,
				Version:  req.Version,
				Resource: req.Resource,
			},
			Namespace:   req.Namespace,
			DeleteRatio: 1.0,
		}
	default:
		return nil
	}

	builder, err := createRequestBuilderFunc(weightedReq, e.spec.MaxRetries)
	if err != nil {
		return nil
	}
	return builder
}
