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

import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.provider.ConfigProvider;

import java.util.Map;
import java.util.Set;

public class IsolatedConfigProvider extends IsolatedPlugin<ConfigProvider> implements AutoCloseable {

    IsolatedConfigProvider(Plugins plugins, ConfigProvider delegate) {
        super(plugins, delegate, PluginType.CONFIGPROVIDER);
    }

    public void configure(Map<String, ?> configs) throws Exception {
        isolateV(delegate::configure, configs);
    }

    public ConfigData get(String path) throws Exception {
        return isolate(delegate::get, path);
    }

    public ConfigData get(String path, Set<String> keys) throws Exception {
        return isolate(delegate::get, path, keys);
    }

    public void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) throws Exception {
        isolateV(() -> delegate.subscribe(path, keys, callback));
    }

    public void unsubscribe(String path, Set<String> keys, ConfigChangeCallback callback) throws Exception {
        isolateV(() -> delegate.unsubscribe(path, keys, callback));
    }

    public void unsubscribeAll() throws Exception {
        isolateV(delegate::unsubscribeAll);
    }

    public void close() throws Exception {
        isolateV(delegate::close);
    }
}
