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
package org.apache.kafka.test;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A JUnit5 extension which tests for socket leaks in test code.
 */
public class LeakTesterExtension {

    /**
     * This class applies a coarse leak test for a whole class at a time.
     * This is automatically loaded for all classes without explicit inclusion.
     * See {@link org.junit.jupiter.api.extension.Extension} ServiceLoader manifest.
     */
    public static class All implements BeforeAllCallback, AfterAllCallback {

        private final CombinedLeakTester tester = new CombinedLeakTester();

        @Override
        public void beforeAll(ExtensionContext extensionContext) throws Exception {
            tester.before();
        }

        @Override
        public void afterAll(ExtensionContext extensionContext) throws Exception {
            tester.after();
        }
    }

    /**
     * This class applies a fine leak test for individual tests.
     * This must be included explicitly with {@link org.junit.jupiter.api.extension.ExtendWith}
     */
    public static class Each implements BeforeEachCallback, AfterEachCallback {

        private final CombinedLeakTester tester = new CombinedLeakTester();
        @Override
        public void beforeEach(ExtensionContext extensionContext) throws Exception {
            tester.before();
        }

        @Override
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            tester.after();
        }
    }
}
