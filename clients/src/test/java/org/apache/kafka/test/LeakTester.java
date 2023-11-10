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

import org.mockito.internal.util.concurrent.WeakConcurrentMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class LeakTester<T> {

    private final WeakConcurrentMap<T, Exception> refs = new WeakConcurrentMap.WithInlinedExpunction<>();
    private final Predicate<T> isOpen;

    public LeakTester(Predicate<T> isOpen) {
        this.isOpen = isOpen;
    }

    public T add(T obj) {
        try {
            throw new Exception("Instantiated " + obj);
        } catch (Exception e) {
            refs.put(obj, e);
        }
        return obj;
    }

    public Set<Exception> live() {
        Set<Exception> ret = new HashSet<>();
        for (Map.Entry<T, Exception> entry : refs) {
            if (isOpen.test(entry.getKey())) {
                ret.add(entry.getValue());
            }
        }
        return ret;
    }

    public Supplier<Set<Exception>> start() {
        Set<Exception> before = live();
        return () -> {
            Set<Exception> after = live();
            after.removeAll(before);
            return after;
        };
    }

    @SafeVarargs
    public static Supplier<Set<Exception>> combine(Supplier<Set<Exception>> ...suppliers) {
        return () -> {
            Set<Exception> leaks = new HashSet<>();
            for (Supplier<Set<Exception>> supplier : suppliers) {
                leaks.addAll(supplier.get());
            }
            return leaks;
        };
    }

    public static void assertEmpty(Set<Exception> leaks) {
        if (!leaks.isEmpty()) {
            RuntimeException e = new RuntimeException("Sockets left open");
            for (Exception leakedSocket : leaks) {
                e.addSuppressed(leakedSocket);
            }
            throw e;
        }
    }
}
