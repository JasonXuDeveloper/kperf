// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package replay

import (
	"os"
	"testing"
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
