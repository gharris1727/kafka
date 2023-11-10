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

import java.util.Set;
import java.util.function.Supplier;

public class CombinedLeakTester {

    private static final SocketLeakTester SOCKET_TESTER = new SocketLeakTester();
    private static final NioLeakTester CHANNEL_TESTER = new NioLeakTester();
    private static final LsofLeakTester LSOF_TESTER = new LsofLeakTester();
    private Supplier<Set<Exception>> leaks;

    public void before() {
        leaks = LeakTester.combine(
                SOCKET_TESTER.start(),
                CHANNEL_TESTER.start(),
                LSOF_TESTER.start()
        );
    }

    public void after() {
        LeakTester.assertEmpty(leaks.get());
    }
}
