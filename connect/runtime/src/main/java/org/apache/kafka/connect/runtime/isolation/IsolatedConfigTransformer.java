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

import org.apache.kafka.common.config.ConfigChangeCallback;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigTransformerResult;
import org.apache.kafka.common.config.provider.ConfigProvider;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class IsolatedConfigTransformer {

    private final ConfigTransformer delegate;

    public IsolatedConfigTransformer(Map<String, IsolatedConfigProvider> configProviders) {
        Objects.requireNonNull(configProviders, "config providers must be non-null");
        Map<String, ConfigProvider> out = new HashMap<>();
        for (Map.Entry<String, IsolatedConfigProvider> e : configProviders.entrySet()) {
            Objects.requireNonNull(e.getValue(), "config provider must be non-null");
            out.put(e.getKey(), new WrappedConfigProvider(e.getValue()));
        }
        this.delegate = new ConfigTransformer(out);
    }

    private static class WrappedException extends RuntimeException {

        private final Exception cause;

        public WrappedException(Exception cause) {
            super(cause);
            this.cause = cause;
        }
    }

    private static class WrappedConfigProvider implements ConfigProvider {

        private final IsolatedConfigProvider delegate;

        private WrappedConfigProvider(IsolatedConfigProvider delegate) {
            this.delegate = delegate;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            try {
                delegate.configure(configs);
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }

        @Override
        public ConfigData get(String path) {
            try {
                return delegate.get(path);
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }

        @Override
        public ConfigData get(String path, Set<String> keys) {
            try {
                return delegate.get(path, keys);
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }

        @Override
        public void subscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
            try {
                delegate.subscribe(path, keys, callback);
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }

        @Override
        public void unsubscribe(String path, Set<String> keys, ConfigChangeCallback callback) {
            try {
                delegate.unsubscribe(path, keys, callback);
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }

        @Override
        public void unsubscribeAll() {
            try {
                delegate.unsubscribeAll();
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } catch (Exception e) {
                throw new WrappedException(e);
            }
        }
    }

    public ConfigTransformerResult transform(Map<String, String> configs) throws Exception {
        try {
            return delegate.transform(configs);
        } catch (WrappedException e) {
            throw e.cause;
        }
    }
}
