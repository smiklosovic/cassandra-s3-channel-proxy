/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.storage.s3.e2e;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * A single real Cassandra node, launched from a source checkout with bin/cassandra.
 * <p>
 * Not an in-JVM dtest on purpose: the thing under test is the plugin classloader loading a provider out of a
 * directory during {@code CassandraDaemon.setup()}, which only happens in a real daemon startup.
 * <p>
 * Nothing is written inside the checkout. CASSANDRA_CONF, CASSANDRA_LOG_DIR and every directory named in the
 * generated yaml point into a working directory the test owns, so the checkout stays clean and a failed run
 * leaves its logs behind for inspection rather than corrupting the next one.
 */
class CassandraNode implements AutoCloseable
{
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);
    private static final String STARTUP_MARKER = "Startup complete";

    private final Path home;
    private final Path work;
    private final Path conf;
    private final Path logs;

    private final int nativePort = freePort();
    private final int storagePort = freePort();
    private final int jmxPort = freePort();

    private Process process;
    private int starts;

    CassandraNode(Path home, Path work)
    {
        this.home = home;
        this.work = work;
        this.conf = work.resolve("conf");
        this.logs = work.resolve("logs");
    }

    int nativePort()
    {
        return nativePort;
    }

    Path dataDirectory()
    {
        return work.resolve("data");
    }

    void start(Map<String, String> providerOptions, String providerDir) throws IOException, InterruptedException
    {
        Files.createDirectories(logs);
        copyConf();
        writeYaml(providerOptions, providerDir);

        // logback appends across restarts, so a restart has to wait for a *new* marker rather than any marker.
        int before = countStartupMarkers();
        launch();
        awaitStartup(before);
    }

    /** Copies the whole conf directory, then redirects the one JMX port that is hardcoded in a shell script. */
    private void copyConf() throws IOException
    {
        Files.createDirectories(conf);
        try (Stream<Path> files = Files.list(home.resolve("conf")))
        {
            for (Path file : files.filter(Files::isRegularFile).collect(Collectors.toList()))
                Files.copy(file, conf.resolve(file.getFileName().toString()),
                           java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        }

        Path env = conf.resolve("cassandra-env.sh");
        Files.write(env, new String(Files.readAllBytes(env), StandardCharsets.UTF_8)
                             .replace("configure_jmx 7199", "configure_jmx " + jmxPort)
                             .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Rewrites the stock cassandra.yaml rather than carrying a copy of it, so the test keeps working as upstream
     * defaults change and only states what this test actually needs to differ.
     */
    @SuppressWarnings("unchecked")
    private void writeYaml(Map<String, String> providerOptions, String providerDir) throws IOException
    {
        Map<String, Object> yaml;
        try (java.io.InputStream in = Files.newInputStream(home.resolve("conf/cassandra.yaml")))
        {
            yaml = (Map<String, Object>) new Yaml().load(in);
        }

        yaml.put("cluster_name", "s3-provider-e2e");
        yaml.put("listen_address", "127.0.0.1");
        yaml.put("rpc_address", "127.0.0.1");
        yaml.put("native_transport_port", nativePort);
        yaml.put("storage_port", storagePort);

        yaml.put("data_file_directories", Arrays.asList(dataDirectory().toString()));
        yaml.put("commitlog_directory", work.resolve("commitlog").toString());
        yaml.put("hints_directory", work.resolve("hints").toString());
        yaml.put("saved_caches_directory", work.resolve("saved_caches").toString());
        yaml.put("cdc_raw_directory", work.resolve("cdc_raw").toString());

        // The provider cannot memory-map an object store, and mmap_index_only is the default.
        yaml.put("disk_access_mode", "standard");
        // Keeps sstable counts predictable: a test flushes and compacts when it means to.
        yaml.put("auto_snapshot", false);

        Map<String, Object> seeds = new LinkedHashMap<>();
        seeds.put("class_name", "org.apache.cassandra.locator.SimpleSeedProvider");
        seeds.put("parameters", Arrays.asList(java.util.Collections.singletonMap("seeds", "127.0.0.1:" + storagePort)));
        yaml.put("seed_provider", Arrays.asList(seeds));

        // Config field names reach yaml as snake_case (DefaultLoader runs them through camelToSnake), so the
        // Java fields storageProviderConfig and pluginDir are configured under these names. Map *contents* are
        // not converted, which is why the provider's own option keys pass through verbatim.
        Map<String, Object> provider = new LinkedHashMap<>();
        provider.put("plugin_dir", providerDir);
        provider.put("options", new LinkedHashMap<>(providerOptions));
        yaml.put("storage_provider_config", provider);

        DumperOptions dumper = new DumperOptions();
        dumper.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        Files.write(conf.resolve("cassandra.yaml"), new Yaml(dumper).dump(yaml).getBytes(StandardCharsets.UTF_8));
    }

    private void launch() throws IOException
    {
        ProcessBuilder builder = new ProcessBuilder(home.resolve("bin/cassandra").toString(), "-f");
        builder.environment().put("CASSANDRA_HOME", home.toString());
        builder.environment().put("CASSANDRA_CONF", conf.toString());
        builder.environment().put("CASSANDRA_LOG_DIR", logs.toString());
        builder.environment().put("MAX_HEAP_SIZE", "1G");
        builder.environment().put("HEAP_NEWSIZE", "256M");
        builder.redirectErrorStream(true);
        starts++;
        builder.redirectOutput(stdout().toFile());

        process = builder.start();
    }

    private void awaitStartup(int markersBefore) throws IOException, InterruptedException
    {
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline)
        {
            if (!process.isAlive())
                throw new IOException("Node exited with " + process.exitValue() + "; output:\n" + tail(stdout(), 80));

            if (countStartupMarkers() > markersBefore)
                return;

            Thread.sleep(500);
        }

        throw new IOException("Node did not start within " + STARTUP_TIMEOUT + "; output:\n" + tail(stdout(), 80));
    }

    private int countStartupMarkers() throws IOException
    {
        int count = 0;
        for (int index = read(logs.resolve("system.log")).indexOf(STARTUP_MARKER);
             index >= 0;
             index = read(logs.resolve("system.log")).indexOf(STARTUP_MARKER, index + 1))
            count++;
        return count;
    }

    boolean logContains(String text) throws IOException
    {
        return read(stdout()).contains(text) || read(logs.resolve("system.log")).contains(text)
               || read(logs.resolve("debug.log")).contains(text);
    }

    /** Waits for a debug.log line containing all parts. */
    void awaitLog(Duration timeout, String... parts) throws IOException, InterruptedException
    {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline)
        {
            for (String line : read(logs.resolve("debug.log")).split("\n"))
                if (Arrays.stream(parts).allMatch(line::contains))
                    return;
            Thread.sleep(250);
        }
        throw new AssertionError("no debug.log line containing " + Arrays.toString(parts) + " within " + timeout);
    }

    /** Runs nodetool against this node, failing if it does not succeed. */
    String nodetool(String... args) throws IOException, InterruptedException
    {
        List<String> command = new ArrayList<>();
        command.add(home.resolve("bin/nodetool").toString());
        command.add("-p");
        command.add(String.valueOf(jmxPort));
        command.addAll(Arrays.asList(args));

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("CASSANDRA_HOME", home.toString());
        builder.environment().put("CASSANDRA_CONF", conf.toString());
        builder.environment().put("CASSANDRA_LOG_DIR", logs.toString());
        builder.redirectErrorStream(true);

        Process nodetool = builder.start();
        String output = new String(readAll(nodetool), StandardCharsets.UTF_8);
        if (!nodetool.waitFor(2, TimeUnit.MINUTES))
        {
            nodetool.destroyForcibly();
            throw new IOException("nodetool " + Arrays.toString(args) + " timed out");
        }
        if (nodetool.exitValue() != 0)
            throw new IOException("nodetool " + Arrays.toString(args) + " failed:\n" + output);

        return output;
    }

    void stop() throws InterruptedException
    {
        if (process == null)
            return;

        process.destroy();
        if (!process.waitFor(2, TimeUnit.MINUTES))
            process.destroyForcibly();

        process = null;
    }

    /** Every file of a component across the data directories, for a test that wants to remove or inspect one. */
    List<Path> componentFiles(String suffix) throws IOException
    {
        List<Path> found = new ArrayList<>();
        Files.walkFileTree(dataDirectory(), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            {
                if (file.getFileName().toString().endsWith(suffix))
                    found.add(file);
                return FileVisitResult.CONTINUE;
            }
        });
        return found;
    }

    @Override
    public void close() throws InterruptedException
    {
        stop();
    }

    private Path stdout()
    {
        return logs.resolve("stdout-" + Math.max(starts, 1) + ".log");
    }

    private static byte[] readAll(Process process) throws IOException
    {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        byte[] chunk = new byte[8192];
        int read;
        while ((read = process.getInputStream().read(chunk)) >= 0)
            out.write(chunk, 0, read);
        return out.toByteArray();
    }

    private static String read(Path file) throws IOException
    {
        return Files.exists(file) ? new String(Files.readAllBytes(file), StandardCharsets.UTF_8) : "";
    }

    private static String tail(Path file, int lines) throws IOException
    {
        List<String> all = Arrays.asList(read(file).split("\n"));
        return String.join("\n", all.subList(Math.max(0, all.size() - lines), all.size()));
    }

    private static int freePort()
    {
        try (ServerSocket socket = new ServerSocket(0))
        {
            return socket.getLocalPort();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not allocate a port", e);
        }
    }
}
