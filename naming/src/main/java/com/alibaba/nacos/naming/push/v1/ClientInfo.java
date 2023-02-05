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

package com.alibaba.nacos.naming.push.v1;

import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.common.utils.StringUtils;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.util.VersionUtil;

/**
 * Client info.
 * 客户端信息
 *
 * @author nacos
 */
public class ClientInfo {

    // 客户端版本信息
    public Version version;

    // 客户端类型（例如Nacos-Java-Client... 、Nacos-C-Client等）
    public ClientType type;

    public ClientInfo(String userAgent) {
        String versionStr = StringUtils.isEmpty(userAgent) ? StringUtils.EMPTY : userAgent;
        // 根据客户端代理判断客户端类型
        this.type = ClientType.getType(versionStr);
        // 如果客户端是c++，则使用c语言类型的客户端类型
        if (versionStr.startsWith(ClientTypeDescription.CPP_CLIENT)) {
            this.type = ClientType.C;
        }
        this.version = parseVersion(versionStr);
    }

    private Version parseVersion(String versionStr) {
        // 如果客户端代理为空，或者客户端是未知的，抛出异常
        if (StringUtils.isBlank(versionStr) || ClientType.UNKNOWN.equals(this.type)) {
            return Version.unknownVersion();
        }
        // 判断客户端版本号，如果小于0则抛出异常
        int versionStartIndex = versionStr.indexOf(":v");
        if (versionStartIndex < 0) {
            return Version.unknownVersion();
        }
        // 返回客户端版本信息
        return VersionUtil.parseVersion(versionStr.substring(versionStartIndex + 2));
    }

    public enum ClientType {
        /**
         * Go client type.
         */
        GO(ClientTypeDescription.GO_CLIENT),
        /**
         * Java client type.
         */
        JAVA(ClientTypeDescription.JAVA_CLIENT),
        /**
         * C client type.
         */
        C(ClientTypeDescription.C_CLIENT),
        /**
         * CSharp client type.
         */
        CSHARP(ClientTypeDescription.CSHARP_CLIENT),
        /**
         * php client type.
         */
        PHP(ClientTypeDescription.PHP_CLIENT),
        /**
         * dns-f client type.
         */
        DNS(ClientTypeDescription.DNSF_CLIENT),
        /**
         * nginx client type.
         */
        TENGINE(ClientTypeDescription.NGINX_CLIENT),
        /**
         * sdk client type.
         */
        JAVA_SDK(ClientTypeDescription.SDK_CLIENT),
        /**
         * Server notify each other.
         */
        NACOS_SERVER(UtilsAndCommons.NACOS_SERVER_HEADER),
        /**
         * Unknown client type.
         */
        UNKNOWN(UtilsAndCommons.UNKNOWN_SITE);

        private final String clientTypeDescription;

        ClientType(String clientTypeDescription) {
            this.clientTypeDescription = clientTypeDescription;
        }

        public String getClientTypeDescription() {
            return clientTypeDescription;
        }

        public static ClientType getType(String userAgent) {
            for (ClientType each : ClientType.values()) {
                if (userAgent.startsWith(each.getClientTypeDescription())) {
                    return each;
                }
            }
            return UNKNOWN;
        }
    }

    public static class ClientTypeDescription {

        public static final String JAVA_CLIENT = "Nacos-Java-Client";

        public static final String DNSF_CLIENT = "Nacos-DNS";

        public static final String C_CLIENT = "Nacos-C-Client";

        public static final String SDK_CLIENT = "Nacos-SDK-Java";

        public static final String NGINX_CLIENT = "unit-nginx";

        public static final String CPP_CLIENT = "vip-client4cpp";

        public static final String GO_CLIENT = "Nacos-Go-Client";

        public static final String PHP_CLIENT = "Nacos-Php-Client";

        public static final String CSHARP_CLIENT = "Nacos-CSharp-Client";
    }

}
