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

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.List;

public class IsolatedSourceTask extends IsolatedTask<SourceTask> {
    IsolatedSourceTask(Plugins plugins, SourceTask delegate) {
        super(plugins, delegate, PluginType.SOURCE_TASK);
    }

    public void initialize(SourceTaskContext context) throws Exception {
        isolateV(delegate::initialize, context);
    }

    public void commit() throws Exception {
        isolateV(delegate::commit);
    }

    @Deprecated
    public void commitRecord(SourceRecord record) throws Exception {
        isolateV(() -> delegate.commitRecord(record));
    }

    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws Exception {
        isolateV(() -> delegate.commitRecord(record, metadata));
    }

    public List<SourceRecord> poll() throws Exception {
        return isolate(delegate::poll);
    }
}
