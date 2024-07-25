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
package org.apache.kafka.connect.util.clusters;

import org.apache.kafka.common.utils.MockExit;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

abstract class EmbeddedConnectBuilder<C extends EmbeddedConnect, B extends EmbeddedConnectBuilder<C, B>> {
    private MockExit exit = MockExit.disallowFatal();
    private Map<String, String> workerProps = new HashMap<>();
    private int numBrokers = EmbeddedConnect.DEFAULT_NUM_BROKERS;
    private Properties brokerProps = new Properties();
    private final Map<String, String> clientProps = new HashMap<>();

    protected abstract C build(
            MockExit exit,
            int numBrokers,
            Properties brokerProps,
            Map<String, String> clientProps,
            Map<String, String> workerProps
    );

    public B exit(MockExit exit) {
        this.exit = exit;
        return self();
    }

    public B workerProps(Map<String, String> workerProps) {
        this.workerProps = workerProps;
        return self();
    }

    public B numBrokers(int numBrokers) {
        this.numBrokers = numBrokers;
        return self();
    }

    public B brokerProps(Properties brokerProps) {
        this.brokerProps = brokerProps;
        return self();
    }

    public B clientProps(Map<String, String> clientProps) {
        this.clientProps.putAll(clientProps);
        return self();
    }

    public C build() {
        return build(exit, numBrokers, brokerProps, clientProps, workerProps);
    }

    @SuppressWarnings("unchecked")
    protected B self() {
        return (B) this;
    }

}
