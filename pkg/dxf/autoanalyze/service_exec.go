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

package autoanalyze

import (
	"context"
	"sync"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/sqlsvrapi"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
)

// This file runs an ANALYZE of a user keyspace on a DXF service node, where the runtime
// of that keyspace has no domain: the runtime allows the tables of the task meta, and
// keyspaceStats is the stats handle of the statement.

// keyspaceStatsPoolSize is the number of idle sessions the stats handle keeps,
// which is more than a statement uses at once: a session that the pool drops
// would stay with the runtime until the statement ends.
const keyspaceStatsPoolSize = 1024

// keyspaceStats is the stats handle of the task keyspace on a node of another
// keyspace, together with the sessions it works on. It lives as long as one
// statement.
type keyspaceStats struct {
	handle  *handle.AnalyzeHandle
	pool    *statementPool
	runtime sqlsvrapi.Runtime
	// globalVars are the global variables of the task keyspace that the
	// statement has read, see statementGlobalVars.
	globalVars sync.Map
	mu         sync.Mutex
	// taken are the sessions taken from the runtime.
	taken []pools.Resource
}

// statementPool is the session pool of the stats handle that serves one
// statement. It can wait until nobody works in a session any more: a killed
// statement returns before the workers that save its results do, and a
// session must not be closed while one of them works in it.
type statementPool struct {
	*syssession.AdvancedSessionPool
	mu      sync.Mutex
	closing bool
	users   sync.WaitGroup
}

func (p *statementPool) enter() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closing {
		return errors.New("the stats handle of the statement is closing")
	}
	p.users.Add(1)
	return nil
}

// WithSession implements syssession.Pool.
func (p *statementPool) WithSession(fn func(*syssession.Session) error) error {
	if err := p.enter(); err != nil {
		return err
	}
	defer p.users.Done()
	return p.AdvancedSessionPool.WithSession(fn)
}

// WithForceBlockGCSession implements syssession.Pool.
func (p *statementPool) WithForceBlockGCSession(ctx context.Context, fn func(*syssession.Session) error) error {
	if err := p.enter(); err != nil {
		return err
	}
	defer p.users.Done()
	return p.AdvancedSessionPool.WithForceBlockGCSession(ctx, fn)
}

// waitIdle makes the pool refuse new users, and waits for the current ones.
func (p *statementPool) waitIdle() {
	p.mu.Lock()
	p.closing = true
	p.mu.Unlock()
	p.users.Wait()
}

// statementGlobalVars keeps the global variables that a statement reads first. Its
// sessions have no sysvar cache, and the stats handle reads a dozen per coprocessor response.
type statementGlobalVars struct {
	variable.GlobalVarAccessor
	values *sync.Map
}

func (v *statementGlobalVars) GetGlobalSysVar(name string) (string, error) {
	if val, ok := v.values.Load(name); ok {
		return val.(string), nil
	}
	val, err := v.GlobalVarAccessor.GetGlobalSysVar(name)
	if err != nil {
		return "", err
	}
	v.values.Store(name, val)
	return val, nil
}

// newKeyspaceStats creates the stats handle. localDom is the domain of the
// current node, which lends its goroutine pool and stats lease.
func newKeyspaceStats(runtime sqlsvrapi.Runtime, localDom *domain.Domain) *keyspaceStats {
	ks := &keyspaceStats{runtime: runtime}
	ks.pool = &statementPool{AdvancedSessionPool: syssession.NewAdvancedSessionPool(keyspaceStatsPoolSize, ks.newSession)}
	ks.handle = handle.NewAnalyzeHandle(ks.pool, localDom.StatsHandle().GPool(), localDom.StatsHandle().Lease())
	return ks
}

// newSession takes a session from the runtime for the stats handle.
func (ks *keyspaceStats) newSession() (syssession.SessionContext, error) {
	res, err := ks.runtime.SysSessionPool().Get()
	if err != nil {
		return nil, errors.Trace(err)
	}
	sctx, ok := res.(syssession.SessionContext)
	if !ok {
		ks.runtime.SysSessionPool().Destroy(res)
		return nil, errors.Errorf("unexpected session type %T", res)
	}
	sessVars := sctx.GetSessionVars()
	sessVars.GlobalVarsAccessor = &statementGlobalVars{GlobalVarAccessor: sessVars.GlobalVarsAccessor, values: &ks.globalVars}
	ks.mu.Lock()
	ks.taken = append(ks.taken, res)
	ks.mu.Unlock()
	return sctx, nil
}

func (ks *keyspaceStats) close() {
	ks.pool.waitIdle()
	ks.closeSessions()
}

// closeSessions closes the sessions, which nobody works in any more, and makes
// them leave the runtime. They are not put back: they carry the settings of the
// statement, and the statement session is bound to this stats handle.
func (ks *keyspaceStats) closeSessions() {
	ks.pool.Close()
	ks.mu.Lock()
	defer ks.mu.Unlock()
	// The pool has closed the sessions. The runtime closes a session that
	// leaves it once more, which does no harm.
	for _, res := range ks.taken {
		ks.runtime.SysSessionPool().Destroy(res)
	}
	ks.taken = nil
}

// runOnServiceNode runs the statement of meta with the runtime of the task
// keyspace, on a node that does not belong to that keyspace.
func runOnServiceNode(
	ctx context.Context,
	runtime sqlsvrapi.Runtime,
	localDom *domain.Domain,
	resource *proto.StepResource,
	meta *TaskMeta,
) error {
	allower, ok := runtime.(sqlsvrapi.UserTableAllower)
	if !ok {
		return errors.Errorf("the runtime %T of the task keyspace cannot load user tables", runtime)
	}
	release, err := allower.AllowUserTable(meta.DBID, meta.TableID)
	if err != nil {
		return errors.Trace(err)
	}
	defer release()

	ks := newKeyspaceStats(runtime, localDom)
	defer ks.close()

	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnStatsForegroundPriority)
	return withStatsSession(ks.pool, func(sctx sessionctx.Context) error {
		domain.BindAnalyzeStatsHandle(sctx, ks.handle)
		// The node's own stats handle only allocates the process ID of the statement.
		return runAnalyze(ctx, sctx, localDom.StatsHandle(), localDom.SysProcTracker(), resource, meta)
	})
}
