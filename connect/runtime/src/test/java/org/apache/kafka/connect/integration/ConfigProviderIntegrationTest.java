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
package org.apache.kafka.connect.integration;

import org.apache.kafka.common.config.internals.AllowedPaths;
import org.apache.kafka.common.config.provider.FileConfigProvider;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertFalse;

public class ConfigProviderIntegrationTest {

    private static final String CONNECTOR_NAME = "connector";
    private static final String SECRET_KEY = "secret-key";
    private static final String SECRET_VALUE = "secret-value";
    private AllowedPaths allowedPaths;
    private String dir;
    private String myFile;
    private String dir2;

    private EmbeddedConnectCluster connect;
    private ConnectorHandle connectorHandle;

    @Before
    public void setup() throws IOException {

        File parent = TestUtils.tempDirectory();
        dir = Files.createDirectory(Paths.get(parent.getAbsolutePath(), "dir")).toString();
        myFile = Files.createFile(Paths.get(dir, "myFile")).toString();
        dir2 = Files.createDirectory(Paths.get(parent.getAbsolutePath(), "dir2")).toString();

        // setup Connect worker properties with secure config providers
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("config.providers", "secure");
        workerProps.put("config.providers.secure.class", FileConfigProvider.class.getName());
        workerProps.put("config.providers.secure.param.allowed.paths", dir2);
        workerProps.put(WorkerConfig.TOPIC_CREATION_ENABLE_CONFIG, "true");

        // write to the secret file we're going to attack
        Properties secrets = new Properties();
        secrets.put(SECRET_KEY, SECRET_VALUE);
        try (FileOutputStream file = new FileOutputStream(myFile)) {
            secrets.store(file, "secret-file");
        }

        // build a Connect cluster backed by Kafka and Zk
        connect = new EmbeddedConnectCluster.Builder()
                .name("connect-cluster")
                .numWorkers(1)
                .numBrokers(1)
                .workerProps(workerProps)
                .build();

        // start the clusters
        connect.start();

        // get a handle to the connector
        connectorHandle = RuntimeHandles.get().connectorHandle(CONNECTOR_NAME);
    }

    @After
    public void teardown() {
        connect.stop();
    }

    @Test
    public void testSecureProvider() throws InterruptedException {
        Map<String, String> connectorConfig = new HashMap<>();
        connectorConfig.put("name", CONNECTOR_NAME);
        connectorConfig.put("connector.class", MonitorableSourceConnector.class.getName());
        connectorConfig.put("topic.creation.default.replication.factor", "1");
        connectorConfig.put("topic.creation.default.partitions", "1");

        connectorConfig.put("config.providers", "insecure");
        connectorConfig.put("config.providers.insecure.class", FileConfigProvider.class.getName());

        String providerToUse = "insecure";
        //String providerToUse = "secure";
        connectorConfig.put("topic", "${" + providerToUse + ":" + myFile + ":" + SECRET_KEY + "}");

        String s = connect.configureConnector(CONNECTOR_NAME, connectorConfig);
        Thread.sleep(10_000);
        assertFalse("secret should not be leaked as topic name",
                connect.connectorTopics(CONNECTOR_NAME).topics().contains(SECRET_VALUE));
    }
}
