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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

import java.util.Map;

public class IsolatedConverter extends IsolatedPlugin<Converter> {

    IsolatedConverter(Plugins plugins, Converter delegate) {
        super(plugins, delegate, PluginType.CONVERTER);
    }

    public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) throws Exception {
        return isolate(() -> delegate.fromConnectData(topic, headers, schema, value));
    }

    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) throws Exception {
        return isolate(() -> delegate.toConnectData(topic, headers, value));
    }

    public ConfigDef config() throws Exception {
        return isolate(delegate::config);
    }

    public void configure(Map<String, ?> configs, boolean isKey) throws Exception {
        isolateV(delegate::configure, configs, isKey);
    }

    public byte[] fromConnectData(String topic, Schema schema, Object value) throws Exception {
        return isolate(() -> delegate.fromConnectData(topic, schema, value));
    }

    public SchemaAndValue toConnectData(String topic, byte[] value) throws Exception {
        return isolate(delegate::toConnectData, topic, value);
    }
}
