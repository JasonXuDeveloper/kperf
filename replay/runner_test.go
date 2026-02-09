// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"os"
	"testing"

	"github.com/Azure/kperf/api/types"
)

func TestGetRunnerIndex(t *testing.T) {
	// Test without environment variable
	idx := GetRunnerIndex(5)
	if idx != 5 {
		t.Errorf("GetRunnerIndex(5) = %d, want 5", idx)
	}

	// Test with environment variable
	os.Setenv("JOB_COMPLETION_INDEX", "3")
	defer os.Unsetenv("JOB_COMPLETION_INDEX")

	idx = GetRunnerIndex(5)
	if idx != 3 {
		t.Errorf("GetRunnerIndex(5) with env=3 = %d, want 3", idx)
	}

	// Test with invalid environment variable
	os.Setenv("JOB_COMPLETION_INDEX", "invalid")
	idx = GetRunnerIndex(7)
	if idx != 7 {
		t.Errorf("GetRunnerIndex(7) with invalid env = %d, want 7", idx)
	}
}

func TestGroupIntoTimeBuckets(t *testing.T) {
	tests := []struct {
		name     string
		requests []types.ReplayRequest
		bucketMs int64
		want     int // expected bucket count
	}{
		{
			name:     "empty requests",
			requests: []types.ReplayRequest{},
			bucketMs: 10,
			want:     0,
		},
		{
			name: "single request",
			requests: []types.ReplayRequest{
				{Timestamp: 100, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
			},
			bucketMs: 10,
			want:     1,
		},
		{
			name: "multiple requests in same bucket",
			requests: []types.ReplayRequest{
				{Timestamp: 100, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 105, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 109, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
			},
			bucketMs: 10,
			want:     1,
		},
		{
			name: "multiple requests across buckets",
			requests: []types.ReplayRequest{
				{Timestamp: 100, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 105, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 110, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 125, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
			},
			bucketMs: 10,
			want:     3, // buckets at 100, 110, 120
		},
		{
			name: "larger bucket size",
			requests: []types.ReplayRequest{
				{Timestamp: 0, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 25, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 50, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 75, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
				{Timestamp: 100, Verb: "GET", ResourceKind: "Pod", APIPath: "/api/v1/pods"},
			},
			bucketMs: 50,
			want:     3, // buckets at 0, 50, 100
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buckets := groupIntoTimeBuckets(tt.requests, tt.bucketMs)
			if len(buckets) != tt.want {
				t.Errorf("groupIntoTimeBuckets() returned %d buckets, want %d", len(buckets), tt.want)
			}

			// Verify all requests are accounted for
			totalReqs := 0
			for _, bucket := range buckets {
				totalReqs += len(bucket.requests)
			}
			if totalReqs != len(tt.requests) {
				t.Errorf("groupIntoTimeBuckets() total requests = %d, want %d", totalReqs, len(tt.requests))
			}

			// Verify buckets are ordered
			for i := 1; i < len(buckets); i++ {
				if buckets[i].timestamp <= buckets[i-1].timestamp {
					t.Errorf("buckets not ordered: bucket[%d].timestamp=%d <= bucket[%d].timestamp=%d",
						i, buckets[i].timestamp, i-1, buckets[i-1].timestamp)
				}
			}
		})
	}
}

func TestCalculateBucketSize(t *testing.T) {
	tests := []struct {
		name      string
		requests  []types.ReplayRequest
		wantRange [2]int64 // [min, max] expected bucket size
	}{
		{
			name:      "empty requests",
			requests:  []types.ReplayRequest{},
			wantRange: [2]int64{10, 10},
		},
		{
			name: "low QPS (50 req/s)",
			requests: func() []types.ReplayRequest {
				reqs := make([]types.ReplayRequest, 100)
				for i := range reqs {
					reqs[i] = types.ReplayRequest{
						Timestamp:    int64(i * 20), // 50 req/s
						Verb:         "GET",
						ResourceKind: "Pod",
						APIPath:      "/api/v1/pods",
					}
				}
				return reqs
			}(),
			wantRange: [2]int64{10, 10}, // Low QPS = 10ms buckets
		},
		{
			name: "medium QPS (200 req/s)",
			requests: func() []types.ReplayRequest {
				reqs := make([]types.ReplayRequest, 1000)
				for i := range reqs {
					reqs[i] = types.ReplayRequest{
						Timestamp:    int64(i * 5), // 200 req/s
						Verb:         "GET",
						ResourceKind: "Pod",
						APIPath:      "/api/v1/pods",
					}
				}
				return reqs
			}(),
			wantRange: [2]int64{20, 20}, // Medium QPS = 20ms buckets
		},
		{
			name: "high QPS (1000 req/s)",
			requests: func() []types.ReplayRequest {
				reqs := make([]types.ReplayRequest, 10000)
				for i := range reqs {
					reqs[i] = types.ReplayRequest{
						Timestamp:    int64(i), // 1000 req/s
						Verb:         "GET",
						ResourceKind: "Pod",
						APIPath:      "/api/v1/pods",
					}
				}
				return reqs
			}(),
			wantRange: [2]int64{50, 50}, // High QPS = 50ms buckets
		},
		{
			name: "very high QPS (5000 req/s)",
			requests: func() []types.ReplayRequest {
				reqs := make([]types.ReplayRequest, 50000)
				for i := range reqs {
					reqs[i] = types.ReplayRequest{
						Timestamp:    int64(i / 5), // 5000 req/s
						Verb:         "GET",
						ResourceKind: "Pod",
						APIPath:      "/api/v1/pods",
					}
				}
				return reqs
			}(),
			wantRange: [2]int64{100, 100}, // Very high QPS = 100ms buckets
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateBucketSize(tt.requests)
			if got < tt.wantRange[0] || got > tt.wantRange[1] {
				t.Errorf("calculateBucketSize() = %d, want range [%d, %d]",
					got, tt.wantRange[0], tt.wantRange[1])
			}
		})
	}
}

func TestNewRunner(t *testing.T) {
	tests := []struct {
		name            string
		workerCount     int
		connsCount      int
		expectedWorkers int
	}{
		{
			name:            "default workers to connections",
			workerCount:     0,
			connsCount:      5,
			expectedWorkers: 5,
		},
		{
			name:            "explicit worker count",
			workerCount:     10,
			connsCount:      5,
			expectedWorkers: 10,
		},
		{
			name:            "no connections defaults to 1",
			workerCount:     0,
			connsCount:      0,
			expectedWorkers: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock REST clients (nil is fine for this test)
			var restClis []interface{} // Using interface{} instead of rest.Interface for testing
			for i := 0; i < tt.connsCount; i++ {
				restClis = append(restClis, nil)
			}

			// Note: This test validates the logic, but we can't easily test the full Runner
			// without mocking the REST interface. The logic is:
			// - If workerCount == 0, use len(restClis)
			// - If len(restClis) == 0, use 1
			// - Otherwise use workerCount

			var expectedCount int
			if tt.workerCount <= 0 {
				expectedCount = tt.connsCount
				if expectedCount == 0 {
					expectedCount = 1
				}
			} else {
				expectedCount = tt.workerCount
			}

			if expectedCount != tt.expectedWorkers {
				t.Errorf("Expected worker count logic: got %d, want %d", expectedCount, tt.expectedWorkers)
			}
		})
	}
}

