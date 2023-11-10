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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ProtocolFamily;
import java.nio.channels.DatagramChannel;
import java.nio.channels.Pipe;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.SelectorProvider;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class NioLeakTester {
    private static final LeakTester<DatagramChannel> DATAGRAM_TESTER = new LeakTester<>(AbstractInterruptibleChannel::isOpen);
    private static final LeakTester<Pipe> PIPE_TESTER = new LeakTester<>(pipeIsOpen());
    private static final LeakTester<AbstractSelector> SELECTOR_TESTER = new LeakTester<>(Selector::isOpen);
    private static final LeakTester<ServerSocketChannel> SERVER_SOCKET_TESTER = new LeakTester<>(AbstractInterruptibleChannel::isOpen);
    private static final LeakTester<SocketChannel> SOCKET_TESTER = new LeakTester<>(AbstractInterruptibleChannel::isOpen);

    private static Predicate<Pipe> pipeIsOpen() {
        return pipe -> pipe.source().isOpen() || pipe.sink().isOpen();
    }

    public Supplier<Set<Exception>> start() {
        return LeakTester.combine(
                DATAGRAM_TESTER.start(),
                PIPE_TESTER.start(),
                SELECTOR_TESTER.start(),
                SERVER_SOCKET_TESTER.start(),
                SOCKET_TESTER.start()
        );
    }

    public static class LeakCheckingProvider extends SelectorProvider {

        private final SelectorProvider selectorProvider;

        public LeakCheckingProvider() {
            selectorProvider = defaultSelectorProvider();
        }

        @Override
        public DatagramChannel openDatagramChannel() throws IOException {
            return DATAGRAM_TESTER.add(selectorProvider.openDatagramChannel());
        }

        @Override
        public DatagramChannel openDatagramChannel(ProtocolFamily family) throws IOException {
            return DATAGRAM_TESTER.add(selectorProvider.openDatagramChannel(family));
        }

        @Override
        public Pipe openPipe() throws IOException {
            return PIPE_TESTER.add(selectorProvider.openPipe());
        }

        @Override
        public AbstractSelector openSelector() throws IOException {
            return SELECTOR_TESTER.add(selectorProvider.openSelector());
        }

        @Override
        public ServerSocketChannel openServerSocketChannel() throws IOException {
            return SERVER_SOCKET_TESTER.add(selectorProvider.openServerSocketChannel());
        }

        @Override
        public SocketChannel openSocketChannel() throws IOException {
            return SOCKET_TESTER.add(selectorProvider.openSocketChannel());
        }
    }

    private static SelectorProvider defaultSelectorProvider() {
        try {
            Class<?> defaultSelectorProvider = Class.forName("sun.nio.ch.DefaultSelectorProvider");
            Method create = defaultSelectorProvider.getDeclaredMethod("create");
            return (SelectorProvider) create.invoke(null);
        } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Unable to retrieve DefaultSelectorProvider ", e);
        }
    }
}
