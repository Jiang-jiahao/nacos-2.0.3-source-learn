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

package com.alibaba.nacos.core.listener;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.exception.runtime.NacosRuntimeException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.executor.ThreadPoolManager;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.event.ServerConfigChangeEvent;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.file.FileChangeEvent;
import com.alibaba.nacos.sys.file.FileWatcher;
import com.alibaba.nacos.sys.file.WatchFileCenter;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.sys.utils.DiskUtils;
import com.alibaba.nacos.sys.utils.InetUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * init environment config.
 * 启动nacos时执行事件的监听器
 *
 * @author <a href="mailto:huangxiaoyu1018@gmail.com">hxy1991</a>
 * @since 0.5.0
 */
public class StartingApplicationListener implements NacosApplicationListener {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(StartingApplicationListener.class);
    
    private static final String MODE_PROPERTY_KEY_STAND_MODE = "nacos.mode";
    
    private static final String MODE_PROPERTY_KEY_FUNCTION_MODE = "nacos.function.mode";
    
    private static final String LOCAL_IP_PROPERTY_KEY = "nacos.local.ip";
    
    private static final String NACOS_APPLICATION_CONF = "nacos_application_conf";
    
    private static final String NACOS_MODE_STAND_ALONE = "stand alone";
    
    private static final String NACOS_MODE_CLUSTER = "cluster";
    
    private static final String DEFAULT_FUNCTION_MODE = "All";
    
    private static final String DEFAULT_DATABASE = "mysql";
    
    private static final String DATASOURCE_PLATFORM_PROPERTY = "spring.datasource.platform";
    
    private static final String DEFAULT_DATASOURCE_PLATFORM = "";
    
    private static final String DATASOURCE_MODE_EXTERNAL = "external";
    
    private static final String DATASOURCE_MODE_EMBEDDED = "embedded";
    
    private static final Map<String, Object> SOURCES = new ConcurrentHashMap<>();
    
    private ScheduledExecutorService scheduledExecutorService;
    
    private volatile boolean starting;
    
    @Override
    public void starting() {
        //标注正在启动为true
        starting = true;
    }
    
    @Override
    public void environmentPrepared(ConfigurableEnvironment environment) {
        //在用户home下创建nacos/logs、nacos/conf、nacos/data文件
        makeWorkDir();
        //给EnvUtil工具类设置环境属性
        injectEnvironment(environment);
        //加载nacos的配置文件，并命名为nacos_application_conf设置到environment中
        loadPreProperties(environment);
        //初始化系统属性
        initSystemProperty();
    }
    
    @Override
    public void contextPrepared(ConfigurableApplicationContext context) {
        logClusterConf();
        
        logStarting();
    }
    
    @Override
    public void contextLoaded(ConfigurableApplicationContext context) {
    
    }
    
    @Override
    public void started(ConfigurableApplicationContext context) {
        starting = false;
        //启动完成，关闭定时任务
        closeExecutor();
        
        ApplicationUtils.setStarted(true);
        judgeStorageMode(context.getEnvironment());
    }
    
    @Override
    public void running(ConfigurableApplicationContext context) {
    }
    
    @Override
    public void failed(ConfigurableApplicationContext context, Throwable exception) {
        starting = false;
        
        makeWorkDir();
        
        LOGGER.error("Startup errors : ", exception);
        ThreadPoolManager.shutdown();
        WatchFileCenter.shutdown();
        NotifyCenter.shutdown();
        
        closeExecutor();
        
        context.close();
        
        LOGGER.error("Nacos failed to start, please see {} for more details.",
                Paths.get(EnvUtil.getNacosHome(), "logs/nacos.log"));
    }
    
    private void injectEnvironment(ConfigurableEnvironment environment) {
        EnvUtil.setEnvironment(environment);
    }
    
    private void loadPreProperties(ConfigurableEnvironment environment) {
        try {
            //获取配置文件信息，加载到SOURCES中
            SOURCES.putAll(EnvUtil.loadProperties(EnvUtil.getApplicationConfFileResource()));
            //将配置文件设置到environment中，命名为nacos_application_conf
            environment.getPropertySources()
                    .addLast(new OriginTrackedMapPropertySource(NACOS_APPLICATION_CONF, SOURCES));
            //注册监听，配置文件发生改变则马上修改环境属性
            registerWatcher();
        } catch (Exception e) {
            throw new NacosRuntimeException(NacosException.SERVER_ERROR, e);
        }
    }
    
    private void registerWatcher() throws NacosException {
        
        WatchFileCenter.registerWatcher(EnvUtil.getConfPath(), new FileWatcher() {
            @Override
            public void onChange(FileChangeEvent event) {
                try {
                    Map<String, ?> tmp = EnvUtil.loadProperties(EnvUtil.getApplicationConfFileResource());
                    SOURCES.putAll(tmp);
                    NotifyCenter.publishEvent(ServerConfigChangeEvent.newEvent());
                } catch (IOException ignore) {
                    LOGGER.warn("Failed to monitor file ", ignore);
                }
            }
            
            @Override
            public boolean interest(String context) {
                return StringUtils.contains(context, "application.properties");
            }
        });
        
    }
    
    private void initSystemProperty() {
        //判断是否是单机还是集群模式
        if (EnvUtil.getStandaloneMode()) {
            System.setProperty(MODE_PROPERTY_KEY_STAND_MODE, NACOS_MODE_STAND_ALONE);
        } else {
            System.setProperty(MODE_PROPERTY_KEY_STAND_MODE, NACOS_MODE_CLUSTER);
        }
        //如果没有设置则设置设置nacos.function.mode=all，如果为config，则设置nacos.function.mode=config
        //naming则设置为设置nacos.function.mode=naming
        if (EnvUtil.getFunctionMode() == null) {
            System.setProperty(MODE_PROPERTY_KEY_FUNCTION_MODE, DEFAULT_FUNCTION_MODE);
        } else if (EnvUtil.FUNCTION_MODE_CONFIG.equals(EnvUtil.getFunctionMode())) {
            System.setProperty(MODE_PROPERTY_KEY_FUNCTION_MODE, EnvUtil.FUNCTION_MODE_CONFIG);
        } else if (EnvUtil.FUNCTION_MODE_NAMING.equals(EnvUtil.getFunctionMode())) {
            System.setProperty(MODE_PROPERTY_KEY_FUNCTION_MODE, EnvUtil.FUNCTION_MODE_NAMING);
        }
        //设置nacos.local.ip=当前机器ip
        System.setProperty(LOCAL_IP_PROPERTY_KEY, InetUtils.getSelfIP());
    }
    
    private void logClusterConf() {
        //判断是否是单机模式，集群模式则进入
        if (!EnvUtil.getStandaloneMode()) {
            try {
                //读取返回一个ip集合并打印
                List<String> clusterConf = EnvUtil.readClusterConf();
                LOGGER.info("The server IP list of Nacos is {}", clusterConf);
            } catch (IOException e) {
                LOGGER.error("read cluster conf fail", e);
            }
        }
    }
    
    private void closeExecutor() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }
    
    private void makeWorkDir() {
        String[] dirNames = new String[] {"logs", "conf", "data"};
        for (String dirName : dirNames) {
            LOGGER.info("Nacos Log files: {}", Paths.get(EnvUtil.getNacosHome(), dirName).toString());
            try {
                DiskUtils.forceMkdir(new File(Paths.get(EnvUtil.getNacosHome(), dirName).toUri()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    private void logStarting() {
        //判断是否是单机模式，集群模式则进入
        if (!EnvUtil.getStandaloneMode()) {
            //创建定时任务
            scheduledExecutorService = ExecutorFactory
                    .newSingleScheduledExecutorService(new NameThreadFactory("com.alibaba.nacos.core.nacos-starting"));
            
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                if (starting) {
                    LOGGER.info("Nacos is starting...");
                }
            }, 1, 1, TimeUnit.SECONDS);
        }
    }
    
    private void judgeStorageMode(ConfigurableEnvironment env) {
        
        // External data sources are used by default in cluster mode
        boolean useExternalStorage = (DEFAULT_DATABASE.equalsIgnoreCase(env.getProperty(DATASOURCE_PLATFORM_PROPERTY, DEFAULT_DATASOURCE_PLATFORM)));
        
        // must initialize after setUseExternalDB
        // This value is true in stand-alone mode and false in cluster mode
        // If this value is set to true in cluster mode, nacos's distributed storage engine is turned on
        // default value is depend on ${nacos.standalone}
        
        if (!useExternalStorage) {
            boolean embeddedStorage = EnvUtil.getStandaloneMode() || Boolean.getBoolean("embeddedStorage");
            // If the embedded data source storage is not turned on, it is automatically
            // upgraded to the external data source storage, as before
            if (!embeddedStorage) {
                useExternalStorage = true;
            }
        }
        
        LOGGER.info("Nacos started successfully in {} mode. use {} storage",
                System.getProperty(MODE_PROPERTY_KEY_STAND_MODE), useExternalStorage ? DATASOURCE_MODE_EXTERNAL : DATASOURCE_MODE_EMBEDDED);
    }
}
