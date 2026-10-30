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
	"cmp"
	"context"
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"go.uber.org/zap"
)

type schedulerImpl struct {
	*scheduler.BaseScheduler
}

var _ scheduler.Scheduler = (*schedulerImpl)(nil)

func newScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) *schedulerImpl {
	return &schedulerImpl{BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param)}
}

func (s *schedulerImpl) Init() error {
	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

func (*schedulerImpl) OnTick(context.Context, *proto.Task) {}

// OnNextSubtasksBatch generates the only subtask, which carries the task meta as is.
func (*schedulerImpl) OnNextSubtasksBatch(_ context.Context, _ storage.TaskHandle,
	task *proto.Task, _ []string, nextStep proto.Step) ([][]byte, error) {
	if nextStep != proto.AutoAnalyzeStepRun {
		return nil, errors.Errorf("unexpected next step %s", proto.Step2Str(task.Type, nextStep))
	}
	return [][]byte{task.Meta}, nil
}

func (*schedulerImpl) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

// GetEligibleInstances returns the nodes of the target scope, fewest used slots
// first. The framework runs a task of max node count 1 on the first node only,
// so the statement goes to the least busy node; nil falls back to host order.
func (s *schedulerImpl) GetEligibleInstances(ctx context.Context, task *proto.Task) ([]string, error) {
	nodes := s.GetNodesInScope()
	if len(nodes) == 0 {
		return nil, nil
	}
	usedSlots, err := s.GetTaskMgr().GetUsedSlotsOnNodes(ctx)
	if err != nil {
		s.GetLogger().Warn("get used slots on nodes failed, place auto analyze task by host order", zap.Error(err))
		return nil, nil
	}
	return orderByUsedSlots(nodes, usedSlots, task.ID), nil
}

// orderByUsedSlots returns the nodes sorted by used slots, ascending. Nodes with
// equal used slots rotate from their given order by the task ID: tasks started
// together read the same used slots, and would all take the same node otherwise.
func orderByUsedSlots(nodes []string, usedSlots map[string]int, taskID int64) []string {
	n := len(nodes)
	if n == 0 {
		return nil
	}
	shift := int(taskID % int64(n))
	positions := make([]int, n)
	for i := range positions {
		positions[i] = i
	}
	slices.SortFunc(positions, func(a, b int) int {
		return cmp.Or(
			cmp.Compare(usedSlots[nodes[a]], usedSlots[nodes[b]]),
			cmp.Compare((a-shift+n)%n, (b-shift+n)%n),
		)
	})
	ordered := make([]string, 0, n)
	for _, i := range positions {
		ordered = append(ordered, nodes[i])
	}
	return ordered
}

// IsRetryableErr returns false: the priority queue of the stats owner analyzes
// the table again later if the task fails.
func (*schedulerImpl) IsRetryableErr(error) bool {
	return false
}

func (*schedulerImpl) GetNextStep(task *proto.TaskBase) proto.Step {
	if task.Step == proto.StepInit {
		return proto.AutoAnalyzeStepRun
	}
	return proto.StepDone
}

func (*schedulerImpl) OnPrepare(context.Context, storage.TaskHandle, *proto.Task) error {
	return nil
}

// ModifyMeta implements scheduler.Extension. The task meta is never modified.
func (*schedulerImpl) ModifyMeta(oldMeta []byte, _ []proto.Modification) ([]byte, error) {
	return oldMeta, nil
}
