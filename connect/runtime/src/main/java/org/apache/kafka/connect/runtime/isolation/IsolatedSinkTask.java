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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.Collection;
import java.util.Map;

public class IsolatedSinkTask extends IsolatedTask<SinkTask> {

    IsolatedSinkTask(Plugins plugins, SinkTask delegate) {
        super(plugins, delegate, PluginType.SINK_TASK);
    }

    public void initialize(SinkTaskContext context) throws Exception {
        isolateV(delegate::initialize, context);
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) throws Exception {
        isolateV(delegate::flush, currentOffsets);
    }

    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) throws Exception {
        return isolate(delegate::preCommit, currentOffsets);
    }

    public void open(Collection<TopicPartition> partitions) throws Exception {
        isolateV(delegate::open, partitions);
    }

    @Deprecated
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) throws Exception {
        isolateV(delegate::onPartitionsAssigned, partitions);
    }

    public void close(Collection<TopicPartition> partitions) throws Exception {
        isolateV(delegate::close, partitions);
    }

    @Deprecated
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) throws Exception {
        isolateV(delegate::onPartitionsRevoked, partitions);
    }

    public void put(Collection<SinkRecord> records) throws Exception {
        isolateV(delegate::put, records);
    }
}
