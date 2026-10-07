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

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Single node against s3mock: offload on flush and compaction, read back live and after restart. */
@Tag("e2e")
class S3OffloadTest
{
    private static final String BUCKET = "cassandra-e2e";
    private static final String KS = "s3test";
    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static S3Mock s3;
    private static S3Client s3Client;
    private static CassandraNode node;
    private static CqlSession session;
    private static Path work;

    @BeforeAll
    static void start() throws Exception
    {
        Path home = Paths.get(System.getProperty("cassandra.home"));
        assertTrue(Files.isDirectory(home.resolve("conf")), "not a checkout: " + home);
        assertTrue(Files.isDirectory(Paths.get(providerDir())), "no provider dir: " + providerDir());

        s3 = new S3Mock();
        s3.start();
        s3Client = s3.client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());

        work = Paths.get("build", "e2e", "node-" + System.currentTimeMillis());
        Files.createDirectories(work);

        node = new CassandraNode(home, work);
        node.start(options(), providerDir());
        session = connect();
        session.execute("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    }

    @AfterAll
    static void stop() throws Exception
    {
        if (session != null)
            session.close();
        if (node != null)
            node.close();
        if (s3Client != null)
            s3Client.close();
        if (s3 != null)
            s3.stop();
        System.out.println("e2e working directory: " + work);
    }

    @Test
    void providerLoaded() throws Exception
    {
        assertTrue(node.logContains("Storage provider org.apache.cassandra.storage.s3.S3ChannelProxyFactory loaded"));
    }

    @Test
    void flushUploadsData() throws Exception
    {
        String table = table("flushed");
        insert(table, 0, 100);
        node.nodetool("flush", KS, table);

        assertTrue(awaitKeys(table, 1).get(0).endsWith("-Data.db"));

        List<String> all = keys("");
        assertTrue(all.stream().allMatch(key -> key.endsWith("-Data.db")), "non-Data components in s3: " + all);
    }

    @Test
    void compactionReplacesObjects() throws Exception
    {
        String table = table("compacted");
        insert(table, 0, 100);
        node.nodetool("flush", KS, table);
        insert(table, 100, 200);
        node.nodetool("flush", KS, table);
        List<String> inputs = awaitKeys(table, 2);
        for (String key : inputs)
            awaitReopened(key);

        node.nodetool("compact", KS, table);

        List<String> output = awaitKeys(table, 1);
        assertTrue(Collections.disjoint(output, inputs), output + " vs " + inputs);
        assertEquals(200L, count(table));
    }

    /** Digest.crc32 stays local and records the CRC32 of Data.db as written. */
    @Test
    void objectMatchesDigest() throws Exception
    {
        String table = table("exact");
        insert(table, 0, 500);
        node.nodetool("flush", KS, table);

        String key = awaitKeys(table, 1).get(0);
        Path data = node.dataDirectory().resolve(key);
        awaitGone(data);

        Path digest = Paths.get(data.toString().replace("-Data.db", "-Digest.crc32"));
        long recorded = Long.parseLong(new String(Files.readAllBytes(digest), StandardCharsets.UTF_8).trim());

        byte[] object = s3Client.getObjectAsBytes(GetObjectRequest.builder().bucket(BUCKET).key(key).build()).asByteArray();
        CRC32 crc = new CRC32();
        crc.update(object, 0, object.length);
        assertEquals(recorded, crc.getValue());
    }

    /** Reopen after eviction routes reads to S3 without a restart. */
    @Test
    void liveRead() throws Exception
    {
        String table = table("live");
        insert(table, 0, 1);
        node.nodetool("flush", KS, table);

        String key = awaitKeys(table, 1).get(0);
        awaitGone(node.dataDirectory().resolve(key));
        awaitReopened(key);

        assertEquals("v0", value(table, 0));
        node.awaitLog(TIMEOUT, "First read of s3://" + BUCKET + '/' + key);
    }

    /** Startup path, which reopen does not reach: orphan sweep and component recovery from the TOC. */
    @Test
    void readAfterRestart() throws Exception
    {
        String table = table("restarted");
        insert(table, 0, 1);
        node.nodetool("flush", KS, table);

        String key = awaitKeys(table, 1).get(0);
        awaitGone(node.dataDirectory().resolve(key));

        session.close();
        node.stop();
        node.start(options(), providerDir());
        session = connect();

        assertEquals("v0", value(table, 0));
        assertFalse(node.logContains("Removing orphans for"));
    }

    private static String table(String name)
    {
        session.execute("CREATE TABLE " + KS + '.' + name + " (k int PRIMARY KEY, v text)");
        return name;
    }

    private static void insert(String table, int from, int to)
    {
        for (int k = from; k < to; k++)
            session.execute("INSERT INTO " + KS + '.' + table + " (k, v) VALUES (?, ?)", k, "v" + k);
    }

    private static String value(String table, int k)
    {
        Row row = session.execute("SELECT v FROM " + KS + '.' + table + " WHERE k = ?", k).one();
        return row == null ? null : row.getString("v");
    }

    private static long count(String table)
    {
        return session.execute("SELECT count(*) FROM " + KS + '.' + table).one().getLong(0);
    }

    private static CqlSession connect()
    {
        return CqlSession.builder()
                         .addContactPoint(new InetSocketAddress("127.0.0.1", node.nativePort()))
                         .withLocalDatacenter("datacenter1")
                         .build();
    }

    private static String providerDir()
    {
        return System.getProperty("provider.dir");
    }

    private static Map<String, String> options()
    {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("bucket", BUCKET);
        options.put("endpoint", s3.endpoint());
        options.put("region", S3Mock.REGION);
        options.put("path_style_access", "true");
        options.put("access_key", S3Mock.ACCESS_KEY);
        options.put("secret_key", S3Mock.SECRET_KEY);
        return options;
    }

    private static void awaitGone(Path file) throws InterruptedException
    {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (Files.exists(file) && System.nanoTime() < deadline)
            Thread.sleep(250);
        assertFalse(Files.exists(file), "not evicted: " + file);
    }

    /** The key minus "-Data.db" is a suffix of the descriptor the reopen log names. */
    private static void awaitReopened(String key) throws Exception
    {
        node.awaitLog(TIMEOUT, "Reopened", '/' + key.replace("-Data.db", " through"));
    }

    /** Waits for exactly {@code expected} objects of a table; the count can grow and shrink. */
    private static List<String> awaitKeys(String table, int expected) throws InterruptedException
    {
        String prefix = KS + '/' + table + '-';
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<String> keys = keys(prefix);
        while (keys.size() != expected && System.nanoTime() < deadline)
        {
            Thread.sleep(500);
            keys = keys(prefix);
        }
        assertEquals(expected, keys.size(), prefix + " settled at " + keys);
        return keys;
    }

    private static List<String> keys(String prefix)
    {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build())
                       .contents()
                       .stream()
                       .map(S3Object::key)
                       .collect(Collectors.toList());
    }
}
