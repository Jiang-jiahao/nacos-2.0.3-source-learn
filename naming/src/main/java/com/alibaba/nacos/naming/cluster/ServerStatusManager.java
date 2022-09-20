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

package com.alibaba.nacos.naming.cluster;

import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.common.utils.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Optional;

/**
 * Detect and control the working status of local server.
 * 检测和控制本地服务器的工作状态。
 *
 * @author nkorange
 * @since 1.0.0
 */
@Service
public class ServerStatusManager {

    // 一致性服务
    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    // 转换领域
    private final SwitchDomain switchDomain;

    // 服务器状态（默认为启动中，下一个状态通常是UP）
    private ServerStatus serverStatus = ServerStatus.STARTING;
    
    public ServerStatusManager(SwitchDomain switchDomain) {
        this.switchDomain = switchDomain;
    }

    // 激活服务器状态更新器
    @PostConstruct
    public void init() {
        GlobalExecutor.registerServerStatusUpdater(new ServerStatusUpdater());
    }

    // 刷新服务器状态
    private void refreshServerStatus() {
        
        if (StringUtils.isNotBlank(switchDomain.getOverriddenServerStatus())) {
            serverStatus = ServerStatus.valueOf(switchDomain.getOverriddenServerStatus());
            return;
        }

        // 如果一致性服务不可用，则更新状态为down（服务已停用），反之更新为up（服务已启动）
        if (consistencyService.isAvailable()) {
            serverStatus = ServerStatus.UP;
        } else {
            serverStatus = ServerStatus.DOWN;
        }
    }
    
    public ServerStatus getServerStatus() {
        return serverStatus;
    }
    
    public Optional<String> getErrorMsg() {
        return consistencyService.getErrorMsg();
    }

    // 服务器状态更新器
    public class ServerStatusUpdater implements Runnable {
        
        @Override
        public void run() {
            refreshServerStatus();
        }
    }
}
