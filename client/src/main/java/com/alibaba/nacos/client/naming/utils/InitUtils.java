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

package com.alibaba.nacos.client.naming.utils;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.SystemPropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.selector.ExpressionSelector;
import com.alibaba.nacos.api.selector.NoneSelector;
import com.alibaba.nacos.api.selector.SelectorType;
import com.alibaba.nacos.client.utils.ContextPathUtil;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TemplateUtils;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * Init utils.
 *
 * @author liaochuntao
 * @author deshao
 */
public class InitUtils {
    
    private static final String DEFAULT_END_POINT_PORT = "8080";
    
    /**
     * Add a difference to the name naming. This method simply initializes the namespace for Naming. Config
     * initialization is not the same, so it cannot be reused directly.
     *
     * @param properties properties
     * @return namespace
     */
    public static String initNamespaceForNaming(Properties properties) {
        String tmpNamespace = null;
        // 判断在云环境中，是否启用命名空间解析
        String isUseCloudNamespaceParsing = properties.getProperty(PropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                System.getProperty(SystemPropertyKeyConst.IS_USE_CLOUD_NAMESPACE_PARSING,
                        String.valueOf(Constants.DEFAULT_USE_CLOUD_NAMESPACE_PARSING)));
        
        if (Boolean.parseBoolean(isUseCloudNamespaceParsing)) {
            // 先从系统环境变量变量名为ans.namespace获取
            tmpNamespace = TenantUtil.getUserTenantForAns();
            LogUtils.NAMING_LOGGER.info("initializer namespace from System Property : {}", tmpNamespace);
            // 获取不到，则从系统环境变量名为ALIBABA_ALIWARE_NAMESPACE获取
            tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
                @Override
                public String call() {
                    String namespace = System.getenv(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_NAMESPACE);
                    LogUtils.NAMING_LOGGER.info("initializer namespace from System Environment :" + namespace);
                    return namespace;
                }
            });
        }
        // 获取不到，则从系统环境变量名为namespace获取
        tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
            @Override
            public String call() {
                String namespace = System.getProperty(PropertyKeyConst.NAMESPACE);
                LogUtils.NAMING_LOGGER.info("initializer namespace from System Property :" + namespace);
                return namespace;
            }
        });
        // 还是获取不到，则从传入的配置文件中名为namespace获取
        if (StringUtils.isEmpty(tmpNamespace)) {
            tmpNamespace = properties.getProperty(PropertyKeyConst.NAMESPACE);
        }
        // 还是获取不到，则直接返回public
        tmpNamespace = TemplateUtils.stringEmptyAndThenExecute(tmpNamespace, new Callable<String>() {
            @Override
            public String call() {
                return UtilAndComs.DEFAULT_NAMESPACE_ID;
            }
        });
        return tmpNamespace;
    }
    
    /**
     * Init web root context.
     *
     * @param properties properties
     * @since 1.4.1
     */
    public static void initWebRootContext(Properties properties) {
        final String webContext = properties.getProperty(PropertyKeyConst.CONTEXT_PATH);
        TemplateUtils.stringNotEmptyAndThenExecute(webContext, new Runnable() {
            @Override
            public void run() {
                // 如果contextPath属性在配置文件已经被设置，则进行正常化处理（比如说前面没有加/，则自动给加上），补充其他路径（前面都加上contextPath）
                UtilAndComs.webContext = ContextPathUtil.normalizeContextPath(webContext);
                UtilAndComs.nacosUrlBase = UtilAndComs.webContext + "/v1/ns";
                UtilAndComs.nacosUrlInstance = UtilAndComs.nacosUrlBase + "/instance";
            }
        });
        // 系统中如果设置了nacos.naming.web.context，则最终采用它的contextPath
        initWebRootContext();
    }
    
    /**
     * Init web root context.
     */
    @Deprecated
    public static void initWebRootContext() {
        // support the web context with ali-yun if the app deploy by EDAS
        final String webContext = System.getProperty(SystemPropertyKeyConst.NAMING_WEB_CONTEXT);
        TemplateUtils.stringNotEmptyAndThenExecute(webContext, new Runnable() {
            @Override
            public void run() {
                UtilAndComs.webContext = ContextPathUtil.normalizeContextPath(webContext);
                UtilAndComs.nacosUrlBase = UtilAndComs.webContext + "/v1/ns";
                UtilAndComs.nacosUrlInstance = UtilAndComs.nacosUrlBase + "/instance";
            }
        });
    }
    
    /**
     * Init end point.
     *
     * @param properties properties
     * @return end point
     */
    public static String initEndpoint(final Properties properties) {
        if (properties == null) {
            
            return "";
        }
        // Whether to enable domain name resolution rules
        String isUseEndpointRuleParsing = properties.getProperty(PropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                System.getProperty(SystemPropertyKeyConst.IS_USE_ENDPOINT_PARSING_RULE,
                        String.valueOf(ParamUtil.USE_ENDPOINT_PARSING_RULE_DEFAULT_VALUE)));
        
        boolean isUseEndpointParsingRule = Boolean.parseBoolean(isUseEndpointRuleParsing);
        String endpointUrl;
        if (isUseEndpointParsingRule) {
            // Get the set domain name information
            endpointUrl = ParamUtil.parsingEndpointRule(properties.getProperty(PropertyKeyConst.ENDPOINT));
            if (StringUtils.isBlank(endpointUrl)) {
                return "";
            }
        } else {
            endpointUrl = properties.getProperty(PropertyKeyConst.ENDPOINT);
        }
        
        if (StringUtils.isBlank(endpointUrl)) {
            return "";
        }
        
        String endpointPort = TemplateUtils
                .stringEmptyAndThenExecute(System.getenv(PropertyKeyConst.SystemEnv.ALIBABA_ALIWARE_ENDPOINT_PORT),
                        new Callable<String>() {
                            @Override
                            public String call() {
                                
                                return properties.getProperty(PropertyKeyConst.ENDPOINT_PORT);
                            }
                        });
        
        endpointPort = TemplateUtils.stringEmptyAndThenExecute(endpointPort, new Callable<String>() {
            @Override
            public String call() {
                return DEFAULT_END_POINT_PORT;
            }
        });
        
        return endpointUrl + ":" + endpointPort;
    }
    
    /**
     * Register subType for serialization.
     *
     * <p>
     * Now these subType implementation class has registered in static code. But there are some problem for classloader.
     * The implementation class will be loaded when they are used, which will make deserialize before register.
     * </p>
     *
     * <p>
     * 子类实现类中的静态代码串中已经向Jackson进行了注册，但是由于classloader的原因，只有当 该子类被使用的时候，才会加载该类。这可能会导致Jackson先进性反序列化，再注册子类，从而导致 反序列化失败。
     * </p>
     */
    public static void initSerialization() {
        // TODO register in implementation class or remove subType
        JacksonUtils.registerSubtype(NoneSelector.class, SelectorType.none.name());
        JacksonUtils.registerSubtype(ExpressionSelector.class, SelectorType.label.name());
    }
}
