// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package request

import (
	"fmt"

	"github.com/Azure/kperf/api/types"
	"github.com/Azure/kperf/request/executor"
)

func init() {
	// Register the request builder factory with the executor package
	executor.SetRequestBuilderFactory(CreateRequestBuilder)
}

// CreateRequestBuilder creates a RESTRequestBuilder from a WeightedRequest.
// This function is used by executors to create request builders.
func CreateRequestBuilder(r *types.WeightedRequest, maxRetries int) (executor.RESTRequestBuilder, error) {
	var builder executor.RESTRequestBuilder
	switch {
	case r.StaleList != nil:
		builder = newRequestListBuilder(r.StaleList, "0", maxRetries)
	case r.QuorumList != nil:
		builder = newRequestListBuilder(r.QuorumList, "", maxRetries)
	case r.WatchList != nil:
		builder = newRequestWatchListBuilder(r.WatchList, maxRetries)
	case r.StaleGet != nil:
		builder = newRequestGetBuilder(r.StaleGet, "0", maxRetries)
	case r.QuorumGet != nil:
		builder = newRequestGetBuilder(r.QuorumGet, "", maxRetries)
	case r.GetPodLog != nil:
		builder = newRequestGetPodLogBuilder(r.GetPodLog, maxRetries)
	case r.Patch != nil:
		builder = newRequestPatchBuilder(r.Patch, "", maxRetries)
	case r.PostDel != nil:
		builder = newRequestPostDelBuilder(r.PostDel, "", maxRetries)
	default:
		return nil, fmt.Errorf("unsupported request type")
	}
	return builder, nil
}
