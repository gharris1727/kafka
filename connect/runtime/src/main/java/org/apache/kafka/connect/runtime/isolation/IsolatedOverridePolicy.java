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
package org.apache.kafka.connect.runtime.isolation;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigRequest;

import java.util.List;
import java.util.Map;

public class IsolatedOverridePolicy extends IsolatedPlugin<ConnectorClientConfigOverridePolicy> {

    IsolatedOverridePolicy(Plugins plugins, ConnectorClientConfigOverridePolicy delegate) {
        super(plugins, delegate, PluginType.CONNECTOR_CLIENT_CONFIG_OVERRIDE_POLICY);
    }

    public List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest) throws Exception {
        return isolate(delegate::validate, connectorClientConfigRequest);
    }

    public void close() throws Exception {
        isolateV(delegate::close);
    }

    public void configure(Map<String, ?> configs) throws Exception {
        isolateV(delegate::configure, configs);
    }
}
