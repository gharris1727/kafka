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

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.DatagramSocket;
import java.net.DatagramSocketImpl;
import java.net.DatagramSocketImplFactory;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketImpl;
import java.net.SocketImplFactory;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class SocketLeakTester {

    private static final LeakTester<SocketImpl> SOCKET_TESTER = new LeakTester<>(socketOpen(SocketImpl.class));
    private static final LeakTester<DatagramSocketImpl> DATAGRAM_SOCKET_TESTER = new LeakTester<>(socketOpen(DatagramSocketImpl.class));

    static {
        try {
            RecordingSocketFactory factory = new RecordingSocketFactory(socketConstructor());
            Socket.setSocketImplFactory(factory);
            ServerSocket.setSocketFactory(factory);
            DatagramSocket.setDatagramSocketImplFactory(factory);
        } catch (IOException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("Unable to instrument sockets", e);
        }
    }

    public Supplier<Set<Exception>> start() {
        return SOCKET_TESTER.start();
    }

    private static class RecordingSocketFactory implements SocketImplFactory, DatagramSocketImplFactory {

        private final Constructor<SocketImpl> constructor;

        private RecordingSocketFactory(Constructor<SocketImpl> constructor) throws ClassNotFoundException, NoSuchMethodException {
            this.constructor = constructor;
        }

        @Override
        public SocketImpl createSocketImpl() {
            return SOCKET_TESTER.add(newSocketImpl());
        }

        private SocketImpl newSocketImpl() {
            try {
                return constructor.newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private DatagramSocketImpl newDatagramSocketImpl() {
            try {
                Class<?> defaultFactory = Class.forName("java.net.DefaultDatagramSocketImplFactory");
                Method create = defaultFactory.getDeclaredMethod("createDatagramSocketImpl", boolean.class);
                return (DatagramSocketImpl) create.invoke(null, false);
            } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public DatagramSocketImpl createDatagramSocketImpl() {
            return DATAGRAM_SOCKET_TESTER.add(newDatagramSocketImpl());
        }
    }

    @SuppressWarnings("unchecked")
    private static Constructor<SocketImpl> socketConstructor() throws ClassNotFoundException, NoSuchMethodException {
        Class<?> defaultSocketImpl = Class.forName("java.net.SocksSocketImpl");
        Constructor<?> constructor = defaultSocketImpl.getDeclaredConstructor();
        constructor.setAccessible(true);
        return (Constructor<SocketImpl>) constructor;
    }

    private static <T> Predicate<T> socketOpen(Class<T> clazz) {
        Method getFileDescriptor;
        try {
            getFileDescriptor = clazz.getDeclaredMethod("getFileDescriptor");
            getFileDescriptor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to access socket file descriptor field", e);
        }
        return socketImpl -> {
            try {
                FileDescriptor fd = (FileDescriptor) getFileDescriptor.invoke(socketImpl);
                return fd != null;
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("Unable to access socket file descriptor");
            }
        };
    }
}
