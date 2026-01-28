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
	method    string
	verb      string
	url       *url.URL
	maskedURL *url.URL
	body      []byte
	timeout   time.Duration
	restCli   rest.Interface
	apiPath   string
}

// NewReplayRequester creates a new ReplayRequester from a ReplayRequest.
func NewReplayRequester(req types.ReplayRequest, restCli rest.Interface, baseURL string) (*ReplayRequester, error) {
	// Build full URL for metrics tracking
	apiPath := req.APIPath
	if !strings.HasPrefix(apiPath, "/") {
		apiPath = "/" + apiPath
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

// Do executes the request and returns the bytes received.
func (r *ReplayRequester) Do(ctx context.Context) (int64, error) {
	// Build the request using rest.Interface (same pattern as existing kperf)
	var req *rest.Request

	// Parse path components from apiPath
	pathParts := strings.Split(strings.Trim(r.apiPath, "/"), "/")

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

	case "PATCH":
		// Default to strategic merge patch for PATCH verb
		req = r.restCli.Patch(apitypes.StrategicMergePatchType).AbsPath(pathParts...).Body(r.body)

	case "APPLY":
		// Server-side apply uses apply patch type
		req = r.restCli.Patch(apitypes.ApplyPatchType).AbsPath(pathParts...).
			Body(r.body).
			Param("fieldManager", "kperf-replay")

	default:
		req = r.restCli.Get().AbsPath(pathParts...)
	}

	// Add label selector if present
	if q := r.url.Query(); q.Get("labelSelector") != "" {
		req = req.Param("labelSelector", q.Get("labelSelector"))
	}

	// Set timeout
	if r.timeout > 0 {
		req = req.Timeout(r.timeout)
	}

	// Execute and read response
	respBody, err := req.Stream(ctx)
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
