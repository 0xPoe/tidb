// Copyright 2025 PingCAP, Inc.
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

package txn

import (
	"context"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"go.uber.org/zap"
)

type schedulerImpl struct {
	*scheduler.BaseScheduler
	logger *zap.Logger
}

var _ scheduler.Scheduler = (*schedulerImpl)(nil)

func newScheduler(ctx context.Context, task *proto.Task, param scheduler.Param) *schedulerImpl {
	return &schedulerImpl{
		BaseScheduler: scheduler.NewBaseScheduler(ctx, task, param),
	}
}

func (s *schedulerImpl) Init() (err error) {
	s.BaseScheduler.Extension = s
	return s.BaseScheduler.Init()
}

func (s *schedulerImpl) OnTick(context.Context, *proto.Task) {
	s.logger.Info("OnTick")
}

func (s *schedulerImpl) OnDone(context.Context, storage.TaskHandle, *proto.Task) error {
	s.logger.Info("OnDone")
	return nil
}

func (*schedulerImpl) IsRetryableErr(error) bool {
	return false
}

func (*schedulerImpl) GetNextStep(task *proto.TaskBase) proto.Step {
	switch task.Step {
	case proto.StepInit:
		return proto.StepOne
	default:
		// current step must be proto.StepOne
		return proto.StepDone
	}
}
