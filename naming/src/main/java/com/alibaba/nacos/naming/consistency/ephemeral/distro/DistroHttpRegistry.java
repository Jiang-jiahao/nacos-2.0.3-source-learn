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

package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.task.DistroTaskEngineHolder;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpCombinedKeyTaskFailedHandler;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.combined.DistroHttpDelayTaskProcessor;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.component.DistroDataStorageImpl;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.component.DistroHttpAgent;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * Distro http registry.
 *
 * @author xiweng.yy
 */
@Component
public class DistroHttpRegistry {

    // Distro组件持有者
    private final DistroComponentHolder componentHolder;

    // Distro任务执行引擎持有者
    private final DistroTaskEngineHolder taskEngineHolder;

    // Distro协议http请求方式的数据对象
    private final DataStore dataStore;

    // Distro协议映射器
    private final DistroMapper distroMapper;

    // 全局配置
    private final GlobalConfig globalConfig;

    // Distro一致性协议服务
    private final DistroConsistencyServiceImpl consistencyService;

    // 服务成员管理器
    private final ServerMemberManager memberManager;

    public DistroHttpRegistry(DistroComponentHolder componentHolder, DistroTaskEngineHolder taskEngineHolder,
            DataStore dataStore, DistroMapper distroMapper, GlobalConfig globalConfig,
            DistroConsistencyServiceImpl consistencyService, ServerMemberManager memberManager) {
        this.componentHolder = componentHolder;
        this.taskEngineHolder = taskEngineHolder;
        this.dataStore = dataStore;
        this.distroMapper = distroMapper;
        this.globalConfig = globalConfig;
        this.consistencyService = consistencyService;
        this.memberManager = memberManager;
    }

    /**
     * Register necessary component to distro protocol for HTTP implement.
     */
    @PostConstruct
    public void doRegister() {
        // 注册com.alibaba.nacos.naming.iplist.类型数据的数据仓库实现
        componentHolder.registerDataStorage(KeyBuilder.INSTANCE_LIST_KEY_PREFIX,
                new DistroDataStorageImpl(dataStore, distroMapper));
        // 注册com.alibaba.nacos.naming.iplist.类型数据的数据传输代理对象实现
        componentHolder.registerTransportAgent(KeyBuilder.INSTANCE_LIST_KEY_PREFIX, new DistroHttpAgent(memberManager));
        // 注册com.alibaba.nacos.naming.iplist.类型的失败任务处理器
        componentHolder.registerFailedTaskHandler(KeyBuilder.INSTANCE_LIST_KEY_PREFIX,
                new DistroHttpCombinedKeyTaskFailedHandler(taskEngineHolder));
        // 注册com.alibaba.nacos.naming.iplist.类型的任务处理器
        taskEngineHolder.registerNacosTaskProcessor(KeyBuilder.INSTANCE_LIST_KEY_PREFIX,
                new DistroHttpDelayTaskProcessor(globalConfig, taskEngineHolder));
        // 注册com.alibaba.nacos.naming.iplist.类型的DistroData数据处理器
        componentHolder.registerDataProcessor(consistencyService);
    }
}
