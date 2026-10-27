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

// Package autoanalyze runs auto-analyze statements as DXF tasks: the stats
// owner keeps the priority queue and the concurrency control, and DXF chooses
// the node that runs each statement.
//
// On the classic kernel the statement runs on a TiDB node of the cluster. On
// the next-gen kernel it runs on a DXF service node, which belongs to the
// SYSTEM keyspace and serves the task keyspace through a runtime that only
// knows the system tables, see service_exec.go.
package autoanalyze

// TaskMeta is the meta of both the task and its single subtask.
type TaskMeta struct {
	// SQL is the analyze statement template, such as "analyze table %n.%n".
	SQL string `json:"sql"`
	// Params are the identifiers to fill into SQL.
	Params []string `json:"params"`
	// NeedVersionRewriteWarn tells the node that runs the statement to log that
	// it rewrites version 1 statistics.
	NeedVersionRewriteWarn bool `json:"need_version_rewrite_warn,omitempty"`
	// DBID and TableID identify the table the statement analyzes. A DXF service
	// node that runs the statement for another keyspace loads this table, and
	// only this one, into the info schema of its sessions.
	DBID    int64 `json:"db_id"`
	TableID int64 `json:"table_id"`
}

func (m *TaskMeta) args() []any {
	args := make([]any, 0, len(m.Params))
	for _, p := range m.Params {
		args = append(args, p)
	}
	return args
}
