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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.connect.runtime.isolation.IsolatedConfigProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.CONFIG_RELOAD_ACTION_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.CONFIG_RELOAD_ACTION_NONE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WorkerConfigTransformerTest {

    public static final String MY_KEY = "myKey";
    public static final String MY_CONNECTOR = "myConnector";
    public static final String TEST_KEY = "testKey";
    public static final String TEST_PATH = "testPath";
    public static final String TEST_KEY_WITH_TTL = "testKeyWithTTL";
    public static final String TEST_KEY_WITH_LONGER_TTL = "testKeyWithLongerTTL";
    public static final String TEST_RESULT = "testResult";
    public static final String TEST_RESULT_WITH_TTL = "testResultWithTTL";
    public static final String TEST_RESULT_WITH_LONGER_TTL = "testResultWithLongerTTL";

    @Mock
    private Herder herder;
    @Mock
    private Worker worker;
    @Mock
    private HerderRequest requestId;
    @Mock
    private IsolatedConfigProvider configProvider;
    private WorkerConfigTransformer configTransformer;

    @Before
    public void setup() {
        configTransformer = new WorkerConfigTransformer(worker, Collections.singletonMap("test", configProvider));
    }

    @Test
    public void testReplaceVariable() throws Exception {
        // Setup
        when(configProvider.get(TEST_PATH, Collections.singleton(TEST_KEY)))
                .thenReturn(new ConfigData(Collections.singletonMap(TEST_KEY, TEST_RESULT)));
        // Execution
        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKey}"));

        // Assertions
        assertEquals(TEST_RESULT, result.get(MY_KEY));
    }

    @Test
    public void testReplaceVariableWithTTL() throws Exception {
        // Setup
        when(configProvider.get(TEST_PATH, Collections.singleton(TEST_KEY_WITH_TTL)))
                .thenReturn(new ConfigData(Collections.singletonMap(TEST_KEY_WITH_TTL, TEST_RESULT_WITH_TTL), 1L));

        // Execution
        Map<String, String> props = new HashMap<>();
        props.put(MY_KEY, "${test:testPath:testKeyWithTTL}");
        props.put(CONFIG_RELOAD_ACTION_CONFIG, CONFIG_RELOAD_ACTION_NONE);
        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, props);

        // Assertions
        assertEquals(TEST_RESULT_WITH_TTL, result.get(MY_KEY));
    }

    @Test
    public void testReplaceVariableWithTTLAndScheduleRestart() throws Exception {
        // Setup
        when(worker.herder()).thenReturn(herder);
        when(herder.restartConnector(eq(1L), eq(MY_CONNECTOR), notNull())).thenReturn(requestId);
        when(configProvider.get(TEST_PATH, Collections.singleton(TEST_KEY_WITH_TTL)))
                .thenReturn(new ConfigData(Collections.singletonMap(TEST_KEY_WITH_TTL, TEST_RESULT_WITH_TTL), 1L));

        // Execution
        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithTTL}"));

        // Assertions
        assertEquals(TEST_RESULT_WITH_TTL, result.get(MY_KEY));
        verify(herder).restartConnector(eq(1L), eq(MY_CONNECTOR), notNull());
    }

    @Test
    public void testReplaceVariableWithTTLFirstCancelThenScheduleRestart() throws Exception {
        // Setup
        when(worker.herder()).thenReturn(herder);
        when(herder.restartConnector(eq(1L), eq(MY_CONNECTOR), notNull())).thenReturn(requestId);
        when(herder.restartConnector(eq(10L), eq(MY_CONNECTOR), notNull())).thenReturn(requestId);
        when(configProvider.get(TEST_PATH, Collections.singleton(TEST_KEY_WITH_TTL)))
                .thenReturn(new ConfigData(Collections.singletonMap(TEST_KEY_WITH_TTL, TEST_RESULT_WITH_TTL), 1L));

        // Execution
        Map<String, String> result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithTTL}"));

        // Assertions
        assertEquals(TEST_RESULT_WITH_TTL, result.get(MY_KEY));
        verify(herder).restartConnector(eq(1L), eq(MY_CONNECTOR), notNull());
        when(configProvider.get(TEST_PATH, Collections.singleton(TEST_KEY_WITH_LONGER_TTL)))
                .thenReturn(new ConfigData(Collections.singletonMap(TEST_KEY_WITH_LONGER_TTL, TEST_RESULT_WITH_LONGER_TTL), 10L));

        // Execution
        result = configTransformer.transform(MY_CONNECTOR, Collections.singletonMap(MY_KEY, "${test:testPath:testKeyWithLongerTTL}"));

        // Assertions
        assertEquals(TEST_RESULT_WITH_LONGER_TTL, result.get(MY_KEY));
        verify(requestId, times(1)).cancel();
        verify(herder).restartConnector(eq(10L), eq(MY_CONNECTOR), notNull());
    }

    @Test
    public void testTransformNullConfiguration() throws Exception {
        assertNull(configTransformer.transform(MY_CONNECTOR, null));
    }
}
