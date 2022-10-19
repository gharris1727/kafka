package org.apache.kafka.connect.runtime.standalone;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.*;

public abstract class ConfigLoader implements Closeable {

    private static final Logger glog = LoggerFactory.getLogger(ConfigLoader.class);
    private final Herder herder;
    protected final Set<Path> configFiles;

    public ConfigLoader(Herder herder, Set<Path> configFiles) {
        this.herder = herder;
        this.configFiles = configFiles;
    }

    public abstract void start() throws ConnectException;

    protected void putConnectorConfig(
            Path connectorPropsFile,
            Map<String, String> connectorProps
    ) throws ExecutionException, InterruptedException {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
            if (error != null)
                glog.error("Failed to create job for {}", connectorPropsFile, error);
            else
                glog.info("Created connector {}", info.result().name());
        });
        herder.putConnectorConfig(
                connectorProps.get(ConnectorConfig.NAME_CONFIG),
                connectorProps, false, cb);
        cb.get();
    }

    protected void deleteConnector(String connectorName) throws ExecutionException, InterruptedException {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
            if (error != null)
                glog.error("Failed to delete connector {}", connectorName, error);
            else
                glog.info("Deleted connector {}", connectorName);
        });
        herder.deleteConnectorConfig(connectorName, cb);
        cb.get();
    }

    public static ConfigLoader getLoader(Herder herder, String[] configFiles) {
        Set<Path> configs = deduplicateConfigFiles(configFiles);
        try {
            // TODO: worker configuration to enable dynamic watching
            WatchService watchService = newWatchService();
            if (watchService != null) {
                return new DynamicConfigLoader(herder, configs, watchService);
            }
        } catch (IOException e) {
            glog.error("Unknown error occurred while starting dynamic directory watching", e);
        }
        glog.info("Falling back to static config loading");
        return new StaticConfigLoader(herder, configs);
    }

    private static Set<Path> deduplicateConfigFiles(String[] configFiles) {
        return Stream.of(configFiles)
                .map(Paths::get)
                .collect(Collectors.toSet());
    }

    private static WatchService newWatchService() throws IOException {
        try {
            return FileSystems.getDefault().newWatchService();
        } catch (UnsupportedOperationException e) {
            glog.info("Filesystem does not support dynamic directory watching");
            return null;
        }
    }

    private static class StaticConfigLoader extends ConfigLoader {

        public StaticConfigLoader(Herder herder, Set<Path> configFiles) {
            super(herder, configFiles);
        }

        public void start() throws ConnectException {
            try {
                for (Path connectorPropsFile : configFiles) {
                    Map<String, String> connectorProps = Utils.propsToStringMap(Utils.loadProps(connectorPropsFile.toString()));
                    putConnectorConfig(connectorPropsFile, connectorProps);
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                throw new ConnectException("Unable to load connector configurations", e);
            }
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }

    private static class DynamicConfigLoader extends ConfigLoader implements Runnable {

        private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);
        private final WatchService watchService;
        private final AtomicBoolean running = new AtomicBoolean();
        private final Map<Path, String> runningConnectors = new HashMap<>();
        private final CountDownLatch stopLatch = new CountDownLatch(1);
        private final AtomicReference<Thread> thread = new AtomicReference<>();

        public DynamicConfigLoader(Herder herder, Set<Path> configFiles, WatchService watchService) throws IOException {
            super(herder, configFiles);
            this.watchService = watchService;
        }

        private void scan(Path connectorPropsFile) {
            log.info("Updating connector config in {}", connectorPropsFile);
            String oldName = runningConnectors.get(connectorPropsFile);
            try {
                Map<String, String> connectorProps = Utils.propsToStringMap(Utils.loadProps(connectorPropsFile.toString()));
                String newName = connectorProps.get(ConnectorConfig.NAME_CONFIG);
                if (newName == null || newName.isEmpty()) {
                    throw new IOException("Connector name must be specified");
                }
                // Check for a name change since we last started a connector from this file
                if (oldName != null) {
                    if (!Objects.equals(oldName, newName)) {
                        try {
                            deleteConnector(oldName);
                        } finally {
                            runningConnectors.remove(connectorPropsFile);
                        }
                    }
                }
                putConnectorConfig(connectorPropsFile, connectorProps);
                runningConnectors.put(connectorPropsFile, newName);
            } catch (Throwable e) {
                // Failing to apply a config means that any previously created connector
                // is stale and should be deleted
                if (oldName != null) {
                    try {
                        deleteConnector(oldName);
                    } catch (InterruptedException | ExecutionException ex) {
                        e.addSuppressed(ex);
                    } finally {
                        runningConnectors.remove(connectorPropsFile);
                    }
                }
                log.error("Unable to update connector config from {}", connectorPropsFile, e);
            }
        }

        @Override
        public void close() throws IOException {
            if (running.getAndSet(false)) {
                running.set(false);
                watchService.close();
            }
            if (thread.get() != null) {
                try {
                    stopLatch.await();
                } catch (InterruptedException e) {
                    log.error("Interrupted waiting for ConfigLoader to stop");
                }
            }
        }

        public void start() throws ConnectException {
            try {
                for (Path connectorPropsFile : configFiles) {
                    Path parentDirectory = connectorPropsFile.getParent();
                    parentDirectory.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
                    scan(connectorPropsFile);
                }
                running.set(true);
                thread.set(new Thread(this, "config-directory-watcher"));
                thread.get().start();
            } catch (IOException e) {
                throw new ConnectException("Unable to register watcher paths", e);
            }
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    WatchKey key = watchService.poll(10, TimeUnit.SECONDS);
                    if (key == null) {
                        continue;
                    }
                    // We only register paths to watch, so this cast should always succeed
                    Path parentDirectory = (Path) key.watchable();
                    for (WatchEvent<?> event : key.pollEvents()) {
                        switch (event.kind().name()) {
                            case "ENTRY_CREATE":
                            case "ENTRY_MODIFY":
                            case "ENTRY_DELETE":
                                Path path = parentDirectory.resolve((Path) event.context());
                                if (configFiles.contains(path)) {
                                    scan(path);
                                } else {
                                    log.trace("Ignoring {} event for un-watched file {}", event.kind().name(), path);
                                }
                                break;
                            default:
                                log.warn("Unable to interpret filesystem event {}", event);
                        }
                    }
                    key.reset();
                } catch (ClosedWatchServiceException e) {
                    running.set(false);
                } catch (InterruptedException ignored) {
                    log.debug("Interrupted while waiting for filesystem events");
                }
            }
            stopLatch.countDown();
        }
    }
}
