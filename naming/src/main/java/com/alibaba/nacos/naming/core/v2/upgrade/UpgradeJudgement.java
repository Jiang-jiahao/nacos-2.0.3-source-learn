/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.core.v2.upgrade;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberMetaDataConstants;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.consistency.persistent.ClusterVersionJudgement;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftCore;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeerSet;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.core.v2.upgrade.doublewrite.RefreshStorageDataTask;
import com.alibaba.nacos.naming.core.v2.upgrade.doublewrite.delay.DoubleWriteDelayTaskEngine;
import com.alibaba.nacos.naming.core.v2.upgrade.doublewrite.execute.AsyncServicesCheckTask;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingExecuteTaskDispatcher;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Ability judgement during upgrading.
 * 用于升级过程中的判断
 *
 * @author xiweng.yy
 */
@Component
public class UpgradeJudgement extends Subscriber<MembersChangeEvent> {
    
    /**
     * Only when all cluster upgrade upper 2.0.0, this features is true.
     * 只有当所有集群升级到2.0.0以上时，该特性才成立
     */
    private final AtomicBoolean useGrpcFeatures = new AtomicBoolean(false);
    
    /**
     * Only when all cluster upgrade upper 1.4.0, this features is true.
     * 只有当所有集群升级到1.4.0以上时，该特性才成立
     */
    private final AtomicBoolean useJraftFeatures = new AtomicBoolean(false);

    // 所有服务器是否都是版本2.x的标识
    private final AtomicBoolean all20XVersion = new AtomicBoolean(false);
    
    private final RaftPeerSet raftPeerSet;
    
    private final RaftCore raftCore;
    
    private final ClusterVersionJudgement versionJudgement;
    
    private final ServerMemberManager memberManager;
    
    private final ServiceManager serviceManager;
    
    private final DoubleWriteDelayTaskEngine doubleWriteDelayTaskEngine;
    
    private ScheduledExecutorService upgradeChecker;
    
    private SelfUpgradeChecker selfUpgradeChecker;
    
    private static final int MAJOR_VERSION = 2;
    
    private static final int MINOR_VERSION = 4;
    
    public UpgradeJudgement(RaftPeerSet raftPeerSet, RaftCore raftCore, ClusterVersionJudgement versionJudgement,
            ServerMemberManager memberManager, ServiceManager serviceManager,
            UpgradeStates upgradeStates,
            DoubleWriteDelayTaskEngine doubleWriteDelayTaskEngine) {
        this.raftPeerSet = raftPeerSet;
        this.raftCore = raftCore;
        this.versionJudgement = versionJudgement;
        this.memberManager = memberManager;
        this.serviceManager = serviceManager;
        this.doubleWriteDelayTaskEngine = doubleWriteDelayTaskEngine;
        // 判断是否升级
        Boolean upgraded = upgradeStates.isUpgraded();
        upgraded = upgraded != null && upgraded;
        boolean isStandaloneMode = EnvUtil.getStandaloneMode();
        // 如果是单机模式或者是升级了，则设置以下属性成true
        if (isStandaloneMode || upgraded) {
            useGrpcFeatures.set(true);
            useJraftFeatures.set(true);
            all20XVersion.set(true);
        }
        // 如果不是单机模式，没有升级，则初始化升级检查器
        if (!isStandaloneMode) {
            initUpgradeChecker();
        }
        NotifyCenter.registerSubscriber(this);
    }
    
    private void initUpgradeChecker() {
        // 一般获取默认的自我升级检查器
        selfUpgradeChecker = SelfUpgradeCheckerSpiHolder.findSelfChecker(EnvUtil.getProperty("upgrading.checker.type", "default"));
        upgradeChecker = ExecutorFactory.newSingleScheduledExecutorService(new NameThreadFactory("upgrading.checker"));
        upgradeChecker.scheduleAtFixedRate(() -> {
            // 如果升级了，则停止
            if (isUseGrpcFeatures()) {
                return;
            }
            // 检查是否可以升级
            boolean canUpgrade = checkForUpgrade();
            Loggers.SRV_LOG.info("upgrade check result {}", canUpgrade);
            if (canUpgrade) {
                doUpgrade();
            }
        }, 100L, 5000L, TimeUnit.MILLISECONDS);
    }
    
    @JustForTest
    void setUseGrpcFeatures(boolean value) {
        useGrpcFeatures.set(value);
    }
    
    @JustForTest
    void setUseJraftFeatures(boolean value) {
        useJraftFeatures.set(value);
    }
    
    public boolean isUseGrpcFeatures() {
        return useGrpcFeatures.get();
    }
    
    public boolean isUseJraftFeatures() {
        return useJraftFeatures.get();
    }
    
    public boolean isAll20XVersion() {
        return all20XVersion.get();
    }

    /**
     * 服务器发生改变的时候，会触发该事件，判断服务的版本是否是小于2.0，根据版本来设置upgraded的值，
     * 最后的目的主要是判断是使用{@link com.alibaba.nacos.naming.core.InstanceOperatorClientImpl}
     * 还是 {@link com.alibaba.nacos.naming.core.InstanceOperatorServiceImpl}
     * @param event {@link Event}
     */
    @Override
    public void onEvent(MembersChangeEvent event) {
        if (!event.hasTriggers()) {
            Loggers.SRV_LOG.info("Member change without no trigger. "
                    + "It may be triggered by member lookup on startup. "
                    + "Skip.");
            return;
        }
        Loggers.SRV_LOG.info("member change, event: {}", event);
        for (Member each : event.getTriggers()) {
            Object versionStr = each.getExtendVal(MemberMetaDataConstants.VERSION);
            // come from below 1.3.0
            if (null == versionStr) {
                checkAndDowngrade(false);
                // 等降级完成在进行设置false，防止双写提前进行
                all20XVersion.set(false);
                return;
            }
            Version version = VersionUtil.parseVersion(versionStr.toString());
            if (version.getMajorVersion() < MAJOR_VERSION) {
                checkAndDowngrade(version.getMinorVersion() >= MINOR_VERSION);
                all20XVersion.set(false);
                return;
            }
        }
        all20XVersion.set(true);
    }
    
    private void checkAndDowngrade(boolean jraftFeature) {
        boolean isDowngradeGrpc = useGrpcFeatures.compareAndSet(true, false);
        boolean isDowngradeJraft = useJraftFeatures.getAndSet(jraftFeature);
        if (isDowngradeGrpc && isDowngradeJraft && !jraftFeature) {
            Loggers.SRV_LOG.info("Downgrade to 1.X");
            NotifyCenter.publishEvent(new UpgradeStates.UpgradeStateChangedEvent(false));
            try {
                raftPeerSet.init();
                raftCore.init();
                versionJudgement.reset();
            } catch (Exception e) {
                Loggers.SRV_LOG.error("Downgrade rafe failed ", e);
            }
        }
    }
    
    private boolean checkForUpgrade() {
        // 如果没有升级，则进行检查
        if (!useGrpcFeatures.get()) {
            // 判断当前节点是否有升级的条件（不需要判断版本，因为只有2.0才会有这个代码，才会有准备升级的属性readyToUpgrade）
            // 如果v1和v2的实例数据都一样的多，并且双写引擎没有执行的任务，那么就认为v1和v2数据已经完全同步一样
            boolean selfCheckResult = selfUpgradeChecker.isReadyToUpgrade(serviceManager, doubleWriteDelayTaskEngine);
            Member self = memberManager.getSelf();
            self.setExtendVal(MemberMetaDataConstants.READY_TO_UPGRADE, selfCheckResult);
            memberManager.updateMember(self);
            if (!selfCheckResult) {
                // 如果还没可以升级，表明v1和v2实例不一样，或者引擎中还有没执行完的任务，则在这里开启异步服务检查任务
                // 主要作用：对比v1和v2的实例，删除v1没有，v2中有的实例，使其v1、v2实例同步一致
                // 为什么要开启异步的？因为节点可能有新增实例之后还没有执行双写的时候宕机了的情况，这样双写是不会同步的，
                // 只有在下次更新服务双写的时候会恢复正常，所以这个地方需要开启并去同步数据
                NamingExecuteTaskDispatcher.getInstance().dispatchAndExecuteTask(AsyncServicesCheckTask.class,
                        new AsyncServicesCheckTask(doubleWriteDelayTaskEngine, this));
            }
        }
        // 判断所有的集群是否都可以升级，如果有一个没有准备好则返回false
        boolean result = true;
        for (Member each : memberManager.allMembers()) {
            Object isReadyToUpgrade = each.getExtendVal(MemberMetaDataConstants.READY_TO_UPGRADE);
            result &= null != isReadyToUpgrade && (boolean) isReadyToUpgrade;
        }
        return result;
    }
    
    private void doUpgrade() {
        Loggers.SRV_LOG.info("Upgrade to 2.0.X");
        useGrpcFeatures.compareAndSet(false, true);
        NotifyCenter.publishEvent(new UpgradeStates.UpgradeStateChangedEvent(true));
        useJraftFeatures.set(true);
        refreshPersistentServices();
    }
    
    private void refreshPersistentServices() {
        for (String each : com.alibaba.nacos.naming.core.v2.ServiceManager.getInstance().getAllNamespaces()) {
            for (Service service : com.alibaba.nacos.naming.core.v2.ServiceManager.getInstance().getSingletons(each)) {
                NamingExecuteTaskDispatcher.getInstance()
                        .dispatchAndExecuteTask(service, new RefreshStorageDataTask(service));
            }
        }
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return MembersChangeEvent.class;
    }
    
    /**
     * Shut down.
     */
    @PreDestroy
    public void shutdown() {
        if (null != upgradeChecker) {
            upgradeChecker.shutdownNow();
        }
    }
    
    /**
     * Stop judgement and clear all cache.
     */
    public void stopAll() {
        try {
            Loggers.SRV_LOG.info("Disable Double write, stop and clean v1.x cache and features");
            useGrpcFeatures.set(true);
            NotifyCenter.publishEvent(new UpgradeStates.UpgradeStateChangedEvent(true));
            useJraftFeatures.set(true);
            NotifyCenter.deregisterSubscriber(this);
            doubleWriteDelayTaskEngine.shutdown();
            if (null != upgradeChecker) {
                upgradeChecker.shutdownNow();
            }
            serviceManager.shutdown();
            raftCore.shutdown();
        } catch (NacosException e) {
            Loggers.SRV_LOG.info("Close double write with exception", e);
        }
    }
}
