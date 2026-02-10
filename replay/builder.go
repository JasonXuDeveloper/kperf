// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"context"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/kperf/api/types"

	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

// ReplayRequester builds and executes replay requests using rest.Interface.
// This is consistent with how existing kperf handles requests.
type ReplayRequester struct {
	method              string
	verb                string
	url                 *url.URL
	maskedURL           *url.URL
	body                []byte
	timeout             time.Duration
	restCli             rest.Interface
	apiPath             string
	connectionLatency   float64 // Time to establish connection (for WATCH)
}

// NewReplayRequester creates a new ReplayRequester from a ReplayRequest.
func NewReplayRequester(req types.ReplayRequest, restCli rest.Interface, baseURL string) (*ReplayRequester, error) {
	// Build full URL for metrics tracking
	apiPath := req.APIPath
	if !strings.HasPrefix(apiPath, "/") {
		apiPath = "/" + apiPath
	}

	// Fix malformed URLs: replace first & with ? if no ? is present
	if !strings.Contains(apiPath, "?") && strings.Contains(apiPath, "&") {
		apiPath = strings.Replace(apiPath, "&", "?", 1)
	}

	baseURL = strings.TrimSuffix(baseURL, "/")
	fullURL := baseURL + apiPath

	parsedURL, err := url.Parse(fullURL)
	if err != nil {
		return nil, err
	}

	// Add label selector as query parameter for LIST/WATCH
	if req.LabelSelector != "" && (req.Verb == "LIST" || req.Verb == "WATCH") {
		q := parsedURL.Query()
		q.Set("labelSelector", req.LabelSelector)
		if req.Verb == "WATCH" {
			q.Set("watch", "true")
		}
		parsedURL.RawQuery = q.Encode()
	}

	// Create masked URL for metrics aggregation
	maskedURL := *parsedURL
	if req.Verb == "DELETE" || req.Verb == "PATCH" || req.Verb == "GET" || req.Verb == "CREATE" {
		// Mask the object name for aggregation
		maskedURL.Path = maskLastPathSegment(maskedURL.Path)
	}

	return &ReplayRequester{
		method:    verbToHTTPMethod(req.Verb),
		verb:      req.Verb,
		url:       parsedURL,
		maskedURL: &maskedURL,
		body:      []byte(req.Body),
		restCli:   restCli,
		apiPath:   apiPath,
	}, nil
}

// Method returns the HTTP method.
func (r *ReplayRequester) Method() string {
	return r.method
}

// URL returns the request URL.
func (r *ReplayRequester) URL() *url.URL {
	return r.url
}

// MaskedURL returns the masked URL for metrics aggregation.
func (r *ReplayRequester) MaskedURL() *url.URL {
	return r.maskedURL
}

// Timeout sets the request timeout.
func (r *ReplayRequester) Timeout(timeout time.Duration) {
	r.timeout = timeout
}

// ConnectionLatency returns the time to establish the connection.
// For WATCH operations, this is the time until the first response is received.
// For other operations, this equals the total latency.
func (r *ReplayRequester) ConnectionLatency() float64 {
	return r.connectionLatency
}

// Do executes the request and returns the bytes received.
func (r *ReplayRequester) Do(ctx context.Context) (int64, error) {
	// Build the request using rest.Interface (same pattern as existing kperf)
	var req *rest.Request

	// Parse path components from URL path (without query string)
	pathParts := strings.Split(strings.Trim(r.url.Path, "/"), "/")

	switch r.verb {
	case "GET", "LIST", "WATCH":
		req = r.restCli.Get().AbsPath(pathParts...)
		if r.verb == "WATCH" {
			req = req.Param("watch", "true")
		}

	case "CREATE":
		req = r.restCli.Post().AbsPath(pathParts...).Body(r.body)

	case "DELETE":
		req = r.restCli.Delete().AbsPath(pathParts...)

	case "DELETECOLLECTION":
		// DeleteCollection is a DELETE with collection-level path (e.g., /api/v1/namespaces/foo/pods)
		req = r.restCli.Delete().AbsPath(pathParts...)

	case "PATCH":
		// Choose patch type based on resource type to avoid 415 errors
		// - Built-in resources (/api/v1) support strategic merge patch
		// - CRDs (/apis/<custom-domain>) only support merge patch and json patch
		patchType := apitypes.MergePatchType // Default to JSON merge patch (works for everything)

		if strings.HasPrefix(r.url.Path, "/api/v1/") || strings.HasPrefix(r.url.Path, "/api/v1beta1/") {
			// Built-in Kubernetes resources support strategic merge patch
			patchType = apitypes.StrategicMergePatchType
		}
		// For CRDs (/apis/custom.domain.com/...), use MergePatchType (JSON merge patch)
		// This is the safest option that works for all CRDs and subresources

		req = r.restCli.Patch(patchType).AbsPath(pathParts...).Body(r.body)

	case "APPLY":
		// Server-side apply uses apply patch type
		req = r.restCli.Patch(apitypes.ApplyPatchType).AbsPath(pathParts...).
			Body(r.body).
			Param("fieldManager", "kperf-replay")

	default:
		req = r.restCli.Get().AbsPath(pathParts...)
	}

	// Add all query parameters from the original URL
	for key, values := range r.url.Query() {
		for _, value := range values {
			req = req.Param(key, value)
		}
	}

	// Set timeout (this may override timeout from query params, which is fine)
	if r.timeout > 0 {
		req = req.Timeout(r.timeout)
	}

	// Execute and read response
	// For WATCH operations, track connection establishment time separately
	connectionStart := time.Now()
	respBody, err := req.Stream(ctx)
	connectionEstablished := time.Now()

	// Store connection latency (time to get first response)
	r.connectionLatency = connectionEstablished.Sub(connectionStart).Seconds()

	if err != nil {
		return 0, err
	}
	defer respBody.Close()

	// Discard body but count bytes
	return io.Copy(io.Discard, respBody)
}

// HTTPError represents an HTTP error response.
type HTTPError struct {
	StatusCode int
	Status     string
}

func (e *HTTPError) Error() string {
	return e.Status
}

// verbToHTTPMethod converts a replay verb to HTTP method.
func verbToHTTPMethod(verb string) string {
	switch verb {
	case "CREATE":
		return "POST"
	case "GET":
		return "GET"
	case "LIST":
		return "LIST"
	case "APPLY":
		return "PATCH"
	case "DELETE":
		return "DELETE"
	case "WATCH":
		return "WATCH"
	case "PATCH":
		return "PATCH"
	default:
		return "GET"
	}
}

// maskLastPathSegment replaces the last path segment with :name for aggregation.
func maskLastPathSegment(path string) string {
	if path == "" {
		return path
	}

	// Remove trailing slash if present
	path = strings.TrimSuffix(path, "/")

	lastSlash := strings.LastIndex(path, "/")
	if lastSlash == -1 {
		return ":name"
	}

	return path[:lastSlash+1] + ":name"
}
