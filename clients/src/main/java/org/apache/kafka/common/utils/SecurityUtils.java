/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.SecurityProviderCreator;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;

public class SecurityUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityConfig.class);

    private static final Map<String, ResourceType> NAME_TO_RESOURCE_TYPES;
    private static final Map<String, AclOperation> NAME_TO_OPERATIONS;

    static {
        NAME_TO_RESOURCE_TYPES = new HashMap<>(ResourceType.values().length);
        NAME_TO_OPERATIONS = new HashMap<>(AclOperation.values().length);

        for (ResourceType resourceType : ResourceType.values()) {
            String resourceTypeName = toPascalCase(resourceType.name());
            NAME_TO_RESOURCE_TYPES.put(resourceTypeName, resourceType);
        }
        for (AclOperation operation : AclOperation.values()) {
            String operationName = toPascalCase(operation.name());
            NAME_TO_OPERATIONS.put(operationName, operation);
        }
    }

    public static KafkaPrincipal parseKafkaPrincipal(String str) {
        if (str == null || str.isEmpty()) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        String[] split = str.split(":", 2);

        if (split.length != 2) {
            throw new IllegalArgumentException("expected a string in format principalType:principalName but got " + str);
        }

        return new KafkaPrincipal(split[0], split[1]);
    }

    public static void addConfiguredSecurityProviders(Map<String, ?> configs) {
        String securityProviderClassesStr = (String) configs.get(SecurityConfig.SECURITY_PROVIDERS_CONFIG);
        if (securityProviderClassesStr == null || securityProviderClassesStr.equals("")) {
            return;
        }
        try {
            String[] securityProviderClasses = securityProviderClassesStr.replaceAll("\\s+", "").split(",");
            for (int index = 0; index < securityProviderClasses.length; index++) {
                SecurityProviderCreator securityProviderCreator = (SecurityProviderCreator) Class.forName(securityProviderClasses[index]).newInstance();
                securityProviderCreator.configure(configs);
                Security.insertProviderAt(securityProviderCreator.getProvider(), index + 1);
            }
        } catch (ClassCastException e) {
            LOGGER.error("Creators provided through " + SecurityConfig.SECURITY_PROVIDERS_CONFIG +
                    " are expected to be sub-classes of SecurityProviderCreator");
        } catch (ClassNotFoundException cnfe) {
            LOGGER.error("Unrecognized security provider creator class", cnfe);
        } catch (IllegalAccessException | InstantiationException e) {
            LOGGER.error("Unexpected implementation of security provider creator class", e);
        }
    }

    public static ResourceType resourceType(String name) {
        ResourceType resourceType = NAME_TO_RESOURCE_TYPES.get(name);
        return resourceType == null ? ResourceType.UNKNOWN : resourceType;
    }

    public static AclOperation operation(String name) {
        AclOperation operation = NAME_TO_OPERATIONS.get(name);
        return operation == null ? AclOperation.UNKNOWN : operation;
    }

    private static String toPascalCase(String name) {
        StringBuilder builder = new StringBuilder();
        boolean capitalizeNext = true;
        for (char c : name.toCharArray()) {
            if (c == '_')
                capitalizeNext = true;
            else if (capitalizeNext) {
                builder.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else
                builder.append(Character.toLowerCase(c));
        }
        return builder.toString();
    }
}
