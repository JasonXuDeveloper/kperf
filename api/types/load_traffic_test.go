// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestLoadProfileUnmarshalFromYAML(t *testing.T) {
	in := `
version: 1
description: test
spec:
  conns: 2
  client: 1
  contentType: json
  mode: weighted-random
  modeConfig:
    rate: 100
    total: 10000
    requests:
    - staleGet:
        group: core
        version: v1
        resource: pods
        namespace: default
        name: x1
      shares: 100
    - quorumGet:
        group: core
        version: v1
        resource: configmaps
        namespace: default
        name: x2
      shares: 150
    - staleList:
        group: core
        version: v1
        resource: pods
        namespace: default
        selector: app=x2
        fieldSelector: spec.nodeName=x
      shares: 200
    - quorumList:
        group: core
        version: v1
        resource: configmaps
        namespace: default
        limit: 10000
        selector: app=x3
      shares: 400
    - put:
        group: core
        version: v1
        resource: configmaps
        namespace: kperf
        name: kperf-
        keySpaceSize: 1000
        valueSize: 1024
      shares: 1000
    - getPodLog:
        namespace: default
        name: hello
        container: main
        tailLines: 1000
        limitBytes: 1024
      shares: 10
    - watchList:
        group: core
        version: v1
        resource: pods
        namespace: default
        selector: app=x7
        fieldSelector: spec.nodeName=x
      shares: 25
`

	target := LoadProfile{}
	require.NoError(t, yaml.Unmarshal([]byte(in), &target))
	assert.Equal(t, 1, target.Version)
	assert.Equal(t, "test", target.Description)
	assert.Equal(t, 2, target.Spec.Conns)
	assert.Equal(t, ModeWeightedRandom, target.Spec.Mode)

	// Type assert to WeightedRandomConfig
	wrConfig, ok := target.Spec.ModeConfig.(*WeightedRandomConfig)
	require.True(t, ok, "ModeConfig should be *WeightedRandomConfig")
	require.NotNil(t, wrConfig)

	assert.Equal(t, float64(100), wrConfig.Rate)
	assert.Equal(t, 10000, wrConfig.Total)
	assert.Len(t, wrConfig.Requests, 7)

	assert.Equal(t, 100, wrConfig.Requests[0].Shares)
	assert.NotNil(t, wrConfig.Requests[0].StaleGet)
	assert.Equal(t, "pods", wrConfig.Requests[0].StaleGet.Resource)
	assert.Equal(t, "v1", wrConfig.Requests[0].StaleGet.Version)
	assert.Equal(t, "core", wrConfig.Requests[0].StaleGet.Group)
	assert.Equal(t, "default", wrConfig.Requests[0].StaleGet.Namespace)
	assert.Equal(t, "x1", wrConfig.Requests[0].StaleGet.Name)

	assert.NotNil(t, wrConfig.Requests[1].QuorumGet)
	assert.Equal(t, 150, wrConfig.Requests[1].Shares)

	assert.Equal(t, 200, wrConfig.Requests[2].Shares)
	assert.NotNil(t, wrConfig.Requests[2].StaleList)
	assert.Equal(t, "pods", wrConfig.Requests[2].StaleList.Resource)
	assert.Equal(t, "v1", wrConfig.Requests[2].StaleList.Version)
	assert.Equal(t, "core", wrConfig.Requests[2].StaleList.Group)
	assert.Equal(t, "default", wrConfig.Requests[2].StaleList.Namespace)
	assert.Equal(t, 0, wrConfig.Requests[2].StaleList.Limit)
	assert.Equal(t, "app=x2", wrConfig.Requests[2].StaleList.Selector)
	assert.Equal(t, "spec.nodeName=x", wrConfig.Requests[2].StaleList.FieldSelector)

	assert.NotNil(t, wrConfig.Requests[3].QuorumList)
	assert.Equal(t, 400, wrConfig.Requests[3].Shares)

	assert.Equal(t, 1000, wrConfig.Requests[4].Shares)
	assert.NotNil(t, wrConfig.Requests[4].Put)
	assert.Equal(t, "configmaps", wrConfig.Requests[4].Put.Resource)
	assert.Equal(t, "v1", wrConfig.Requests[4].Put.Version)
	assert.Equal(t, "core", wrConfig.Requests[4].Put.Group)
	assert.Equal(t, "kperf", wrConfig.Requests[4].Put.Namespace)
	assert.Equal(t, "kperf-", wrConfig.Requests[4].Put.Name)
	assert.Equal(t, 1000, wrConfig.Requests[4].Put.KeySpaceSize)
	assert.Equal(t, 1024, wrConfig.Requests[4].Put.ValueSize)

	assert.Equal(t, 10, wrConfig.Requests[5].Shares)
	assert.NotNil(t, wrConfig.Requests[5].GetPodLog)
	assert.Equal(t, "default", wrConfig.Requests[5].GetPodLog.Namespace)
	assert.Equal(t, "hello", wrConfig.Requests[5].GetPodLog.Name)
	assert.Equal(t, "main", wrConfig.Requests[5].GetPodLog.Container)
	assert.Equal(t, int64(1000), *wrConfig.Requests[5].GetPodLog.TailLines)
	assert.Equal(t, int64(1024), *wrConfig.Requests[5].GetPodLog.LimitBytes)

	assert.Equal(t, 25, wrConfig.Requests[6].Shares)
	assert.NotNil(t, wrConfig.Requests[6].WatchList)

	assert.NoError(t, target.Validate())
}

func TestWeightedRequest(t *testing.T) {
	tests := map[string]struct {
		req WeightedRequest
		err bool
	}{
		"shares < 0": {
			req: WeightedRequest{
				Shares: -1,
			},
			err: true,
		},
		"no request setting": {
			req: WeightedRequest{
				Shares: 100,
			},
			err: true,
		},
		"empty version": {
			req: WeightedRequest{
				Shares: 100,
				StaleList: &RequestList{
					KubeGroupVersionResource: KubeGroupVersionResource{
						Resource: "pods",
					},
				},
			},
			err: true,
		},
		"empty resource": {
			req: WeightedRequest{
				Shares: 100,
				StaleList: &RequestList{
					KubeGroupVersionResource: KubeGroupVersionResource{
						Version: "v1",
					},
				},
			},
			err: true,
		},
		"wrong limit": {
			req: WeightedRequest{
				Shares: 100,
				StaleList: &RequestList{
					KubeGroupVersionResource: KubeGroupVersionResource{
						Resource: "pods",
						Version:  "v1",
					},
					Limit: -1,
				},
			},
			err: true,
		},
		"no error": {
			req: WeightedRequest{
				Shares: 100,
				StaleList: &RequestList{
					KubeGroupVersionResource: KubeGroupVersionResource{
						Resource: "pods",
						Version:  "v1",
					},
					Limit: 0,
				},
			},
			err: false,
		},
	}

	for name, tc := range tests {
		tc := tc
		t.Run(name, func(t *testing.T) {
			err := tc.req.Validate()
			if tc.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
