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
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.predicates.Predicate;

import java.util.Map;

public class IsolatedPredicate<R extends ConnectRecord<R>> extends IsolatedPlugin<Predicate<R>> implements AutoCloseable {

    IsolatedPredicate(Plugins plugins, Predicate<R> delegate) {
        super(plugins, delegate, PluginType.PREDICATE);
    }

    public void configure(Map<String, ?> configs) throws Exception {
        isolateV(delegate::configure, configs);
    }

    public ConfigDef config() throws Exception {
        return isolate(delegate::config);
    }

    public boolean test(R record) throws Exception {
        return isolate(delegate::test, record);
    }

    public void close() throws Exception {
        isolateV(delegate::close);
    }
}
