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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LsofLeakTester {

    private static final Logger log = LoggerFactory.getLogger(SocketLeakTester.class);
    private static final long PID = pid();

    public Supplier<Set<Exception>> start() {
        Map<String, String> lastFiles = live();
        return () -> {
            Map<String, String> currentFiles = live();
            Set<Exception> leaks = new HashSet<>();
            for (Map.Entry<String, String> current : currentFiles.entrySet()) {
                if (!lastFiles.containsKey(current.getKey())) {
                    leaks.add(new Exception(current.getValue()));
                }
            }
            return leaks;
        };
    }

    private Map<String, String> live() {
        Map<String, String> ret = new HashMap<>();
        try {
            Process lsof = Runtime.getRuntime().exec(new String[]{"lsof", "-a", "-i", "-p", Long.toString(PID)});
            lsof.getOutputStream().close();
            List<String> err = new ArrayList<>();
            List<String> out = new ArrayList<>();
            Thread errThread = new Thread(new StreamGobbler(lsof.getErrorStream(), err::add));
            Thread outThread = new Thread(new StreamGobbler(lsof.getInputStream(), out::add));
            errThread.start();
            outThread.start();
            lsof.waitFor();
            errThread.join();
            outThread.join();
            if (!err.isEmpty()) {
                log.error("Stderr: {}", err);
            }
            for (int i = 1; i < out.size(); i++) {
                String[] split = out.get(i).split("\\s+", 7);
                String device = split[5];
                String details = split[6];
                ret.put(device, details);
            }
            return ret;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Unable to call lsof", e);
        }
    }

    private static long pid() {
        try {
            Class<?> processHandleClass = Class.forName("java.lang.ProcessHandle");
            Method current = processHandleClass.getDeclaredMethod("current");
            Object processHandle = current.invoke(null);
            Method pid = processHandleClass.getDeclaredMethod("pid");
            return (long) pid.invoke(processHandle);
        } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Unable to get current process ID", e);
        }
    }

    private static class StreamGobbler implements Runnable {
        private final InputStream inputStream;
        private final Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines().forEach(consumer);
        }
    }
}
