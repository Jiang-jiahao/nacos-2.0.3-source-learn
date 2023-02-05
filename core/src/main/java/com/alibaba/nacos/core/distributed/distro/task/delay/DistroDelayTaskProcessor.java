/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.core.distributed.distro.task.delay;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroSyncChangeTask;
import com.alibaba.nacos.core.distributed.distro.task.execute.DistroSyncDeleteTask;

/**
 * Distro delay task processor.
 * Distro延时任务处理器
 *
 * @author xiweng.yy
 */
public class DistroDelayTaskProcessor implements NacosTaskProcessor {

    // Distro任务引擎持有者
    private final DistroTaskEngineHolder distroTaskEngineHolder;

    // Distro组件持有者
    private final DistroComponentHolder distroComponentHolder;

    public DistroDelayTaskProcessor(DistroTaskEngineHolder distroTaskEngineHolder,
            DistroComponentHolder distroComponentHolder) {
        this.distroTaskEngineHolder = distroTaskEngineHolder;
        this.distroComponentHolder = distroComponentHolder;
    }

    @Override
    public boolean process(NacosTask task) {
        // 不处理非延迟任务
        if (!(task instanceof DistroDelayTask)) {
            return true;
        }
        DistroDelayTask distroDelayTask = (DistroDelayTask) task;
        DistroKey distroKey = distroDelayTask.getDistroKey();
        // 根据不同的操作类型创建具体的任务
        switch (distroDelayTask.getAction()) {
            case DELETE:
                DistroSyncDeleteTask syncDeleteTask = new DistroSyncDeleteTask(distroKey, distroComponentHolder);
                distroTaskEngineHolder.getExecuteWorkersManager().addTask(distroKey, syncDeleteTask);
                return true;
            case CHANGE:
            case ADD:
                DistroSyncChangeTask syncChangeTask = new DistroSyncChangeTask(distroKey, distroComponentHolder);
                distroTaskEngineHolder.getExecuteWorkersManager().addTask(distroKey, syncChangeTask);
                return true;
            default:
                return false;
        }
    }
}
