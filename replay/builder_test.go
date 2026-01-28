// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"testing"

	"github.com/Azure/kperf/api/types"
)

func TestVerbToHTTPMethod(t *testing.T) {
	tests := []struct {
		verb string
		want string
	}{
		{"CREATE", "POST"},
		{"GET", "GET"},
		{"LIST", "LIST"},
		{"APPLY", "PATCH"},
		{"DELETE", "DELETE"},
		{"WATCH", "WATCH"},
		{"PATCH", "PATCH"},
		{"UNKNOWN", "GET"}, // default
	}

	for _, tt := range tests {
		t.Run(tt.verb, func(t *testing.T) {
			got := verbToHTTPMethod(tt.verb)
			if got != tt.want {
				t.Errorf("verbToHTTPMethod(%s) = %s, want %s", tt.verb, got, tt.want)
			}
		})
	}
}

func TestMaskLastPathSegment(t *testing.T) {
	tests := []struct {
		path string
		want string
	}{
		{"/api/v1/namespaces/default/pods/nginx", "/api/v1/namespaces/default/pods/:name"},
		{"/api/v1/namespaces/default/pods", "/api/v1/namespaces/default/:name"},
		{"/api/v1/nodes/node-1", "/api/v1/nodes/:name"},
		{"/single", "/:name"},
		{"", ""},
		{"/trailing/", "/:name"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got := maskLastPathSegment(tt.path)
			if got != tt.want {
				t.Errorf("maskLastPathSegment(%s) = %s, want %s", tt.path, got, tt.want)
			}
		})
	}
}

func TestNewReplayRequesterURLBuilding(t *testing.T) {
	// We can't easily test with a real rest.Interface without a running cluster,
	// but we can test the URL building logic by checking the requester fields
	tests := []struct {
		name       string
		req        types.ReplayRequest
		baseURL    string
		wantMethod string
	}{
		{
			name: "GET request",
			req: types.ReplayRequest{
				Verb:         "GET",
				Namespace:    "default",
				ResourceKind: "Pod",
				Name:         "nginx",
				APIPath:      "/api/v1/namespaces/default/pods/nginx",
			},
			baseURL:    "https://kubernetes.default.svc",
			wantMethod: "GET",
		},
		{
			name: "CREATE request",
			req: types.ReplayRequest{
				Verb:         "CREATE",
				Namespace:    "default",
				ResourceKind: "Pod",
				Name:         "nginx",
				APIPath:      "/api/v1/namespaces/default/pods",
				Body:         `{"apiVersion":"v1","kind":"Pod"}`,
			},
			baseURL:    "https://kubernetes.default.svc",
			wantMethod: "POST",
		},
		{
			name: "DELETE request",
			req: types.ReplayRequest{
				Verb:         "DELETE",
				Namespace:    "default",
				ResourceKind: "Pod",
				Name:         "nginx",
				APIPath:      "/api/v1/namespaces/default/pods/nginx",
			},
			baseURL:    "https://kubernetes.default.svc",
			wantMethod: "DELETE",
		},
		{
			name: "LIST request",
			req: types.ReplayRequest{
				Verb:         "LIST",
				Namespace:    "default",
				ResourceKind: "Pod",
				APIPath:      "/api/v1/namespaces/default/pods",
			},
			baseURL:    "https://kubernetes.default.svc",
			wantMethod: "LIST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// We pass nil for restCli since we're only testing URL building
			// The actual request execution would need a real client
			requester, err := NewReplayRequester(tt.req, nil, tt.baseURL)
			if err != nil {
				t.Fatalf("NewReplayRequester() error = %v", err)
			}

			if requester.Method() != tt.wantMethod {
				t.Errorf("Method() = %s, want %s", requester.Method(), tt.wantMethod)
			}

			// Check URL is properly constructed
			expectedURL := tt.baseURL + tt.req.APIPath
			if requester.URL().String() != expectedURL {
				t.Errorf("URL() = %s, want %s", requester.URL().String(), expectedURL)
			}
		})
	}
}

func TestHTTPError(t *testing.T) {
	err := &HTTPError{
		StatusCode: 404,
		Status:     "404 Not Found",
	}

	if err.Error() != "404 Not Found" {
		t.Errorf("Error() = %s, want '404 Not Found'", err.Error())
	}
}
