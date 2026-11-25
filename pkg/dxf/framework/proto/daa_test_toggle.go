// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"os"
	"strconv"
	"strings"
)

// DO NOT MERGE: switches for a cluster test of the auto-analyze interruptions.
const (
	// DAAPlaceFixFile turns on the placement that does not count a task twice.
	DAAPlaceFixFile = "/tmp/daa-place-fix"
	// DAANoPreemptFile keeps running AutoAnalyze subtasks: no preemption by the same
	// priority, and the balancer does not move them off a live node.
	DAANoPreemptFile = "/tmp/daa-no-preempt"
	// DAAAnalyzePriorityFile holds the priority of new AutoAnalyze tasks, read on the node that submits them.
	DAAAnalyzePriorityFile = "/tmp/daa-analyze-priority"
	// DAADrainTimeoutFile holds the seconds that a task of a higher priority waits for running AutoAnalyze
	// tasks to finish before it preempts them. While it waits, the node starts no task of a lower rank.
	DAADrainTimeoutFile = "/tmp/daa-drain-timeout"
)

// DAAIntFile reads the integer in the switch file, and reports whether the file exists and holds one.
func DAAIntFile(file string) (int, bool) {
	b, err := os.ReadFile(file)
	if err != nil {
		return 0, false
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(b)))
	return v, err == nil
}

// DAAToggle reports whether the switch file exists on this node.
func DAAToggle(file string) bool {
	_, err := os.Stat(file)
	return err == nil
}
