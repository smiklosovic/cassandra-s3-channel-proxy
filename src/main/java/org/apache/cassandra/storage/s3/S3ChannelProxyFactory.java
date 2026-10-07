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

package org.apache.cassandra.storage.s3;

import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.service.storage.ChannelProxyFactory;
import org.apache.cassandra.utils.Pair;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Backs sstable reads with S3 objects.
 * <p>
 * Discovered by {@code ServiceLoader} through META-INF/services, loaded in an isolated classloader so that the
 * AWS SDK's Netty and Jackson never meet Cassandra's.
 * <p>
 * Note the shape this is forced into: {@link ChannelProxy} is final, so a provider cannot subclass it. The only
 * extension point is {@link ChannelProxy#ChannelProxy(File, java.nio.channels.FileChannel)} - the provider
 * supplies a {@link java.nio.channels.FileChannel}, and {@link S3FileChannel} is where the real work lives.
 * <p>
 * Reads are local-first. A component still on local disk is served from it, and S3 is consulted only for one
 * that is not - which is what makes a read-only channel coherent at all, since an sstable is written locally and
 * then immediately read back by {@code openEarly}/{@code openFinal} before any upload could have happened.
 * <p>
 * The other half, {@link S3SSTableOffloader}, is reached through {@link #notificationConsumer()} and has to be
 * subscribed by the node, next to the other per-table consumers in {@code ColumnFamilyStore}:
 * <pre>
 *     data.subscribe(StorageService.instance.sstablesTracker);
 *     data.subscribe(SnapshotManager.instance);
 *     StorageProviders.notificationConsumer().ifPresent(data::subscribe);   // &lt;- this provider
 * </pre>
 * Without that line the provider is read-only and nothing is ever uploaded.
 */
public class S3ChannelProxyFactory implements ChannelProxyFactory
{
    /**
     * Components to copy to S3. Data is the overwhelming majority of the bytes; the metadata components stay
     * local so that {@code Directories.SSTableLister} still finds every sstable by listing directories, and so
     * that startup metadata reads and bloom filter lookups do not become network calls.
     */
    private static final String DEFAULT_OFFLOAD = "Data.db";

    private volatile String bucket;
    private volatile String keyPrefix;
    private volatile S3Client client;
    private volatile S3SSTableOffloader offloader;

    @Override
    public void configure(Map<String, String> options)
    {
        this.bucket = options.get("bucket");
        if (bucket == null)
            throw new IllegalArgumentException("s3 provider requires a 'bucket' option");

        requireNonMmappedAccess();

        this.keyPrefix = normalizePrefix(options.get("key_prefix"));

        this.client = buildClient(options);

        Set<String> offloaded = components(options.getOrDefault("offload_components", DEFAULT_OFFLOAD));
        int threads = Integer.parseInt(options.getOrDefault("upload_threads", "2"));

        // The local copy is always dropped once S3 has it: S3 is the home of an offloaded component, not a
        // backup of it. Which components get offloaded is the knob - see offload_components.
        this.offloader = new S3SSTableOffloader(client, bucket, keyPrefix, offloaded, threads);
    }

    /**
     * Builds the client. Everything is optional: with no options at all this is {@code S3Client.create()}, which
     * resolves region and credentials from the environment the way every other AWS tool does.
     * <p>
     * The overrides exist for S3-compatible storage - MinIO, Ceph, a mock in a test - which needs an explicit
     * endpoint and, because such endpoints rarely implement virtual-host-style addressing, path-style access.
     */
    private static S3Client buildClient(Map<String, String> options)
    {
        S3ClientBuilder builder = S3Client.builder();

        String endpoint = options.get("endpoint");
        if (endpoint != null)
            builder.endpointOverride(URI.create(endpoint));

        String region = options.get("region");
        if (region != null)
            builder.region(Region.of(region));

        // Virtual-host-style turns the bucket into a DNS label, which an endpoint reached by IP or by a bare
        // container name cannot serve.
        if (Boolean.parseBoolean(options.getOrDefault("path_style_access", "false")))
            builder.forcePathStyle(true);

        String accessKey = options.get("access_key");
        String secretKey = options.get("secret_key");
        if (accessKey != null && secretKey != null)
            builder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey,
                                                                                                     secretKey)));
        else if (accessKey != null || secretKey != null)
            throw new IllegalArgumentException("s3 provider needs both 'access_key' and 'secret_key', or neither");

        return builder.build();
    }

    /**
     * The consumer that performs the uploads, for the node to subscribe to each table's tracker. Empty until
     * {@link #configure} has run.
     */
    @Override
    public Optional<INotificationConsumer> notificationConsumer()
    {
        return Optional.ofNullable(offloader);
    }

    static Set<String> components(String csv)
    {
        Set<String> names = Arrays.stream(csv.split(","))
                                  .map(String::trim)
                                  .filter(name -> !name.isEmpty())
                                  .collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableSet(names);
    }

    /** A prefix is a key namespace, not a path, so it needs exactly one trailing separator and no leading one. */
    static String normalizePrefix(String prefix)
    {
        if (prefix == null || prefix.isEmpty())
            return "";

        String trimmed = prefix.trim();
        while (trimmed.startsWith("/"))
            trimmed = trimmed.substring(1);

        return trimmed.isEmpty() ? "" : trimmed.endsWith("/") ? trimmed : trimmed + '/';
    }

    /**
     * {@link S3FileChannel} cannot honour {@link java.nio.channels.FileChannel#map}, and the default
     * {@code disk_access_mode} is {@code mmap_index_only}, so a node left on defaults would start cleanly and
     * then fail on its first index read. Checked here, during startup, where the operator can still act on it.
     */
    private static void requireNonMmappedAccess()
    {
        DiskAccessMode mode = DatabaseDescriptor.getDiskAccessMode();
        if (mode == DiskAccessMode.mmap || mode == DiskAccessMode.mmap_index_only)
            throw new ConfigurationException(String.format("The s3 storage provider cannot memory-map its " +
                                                           "sstables, but disk_access_mode is %s. Set " +
                                                           "disk_access_mode: standard.", mode));
    }

    @Override
    public ChannelProxy create(File file, ChannelProxy.IOMode ioMode)
    {
        if (file.exists())
            return new ChannelProxy(file, ioMode);

        String key = keyFor(file);
        if (key == null)
            return new ChannelProxy(file, ioMode); // not an sstable component: let the local path report it missing

        // IOMode is a local-disk concern (buffered vs O_DIRECT) and has no S3 analogue; ignored deliberately.
        return new ChannelProxy(file, new S3FileChannel(client, bucket, key));
    }

    /**
     * Waits for in-flight uploads before dropping the client they need. Without this an sstable flushed during
     * drain could be left with no copy in S3, since the upload threads are daemons and die with the JVM.
     */
    @Override
    public void shutdown()
    {
        S3SSTableOffloader current = offloader;
        if (current != null)
            current.shutdown();

        S3Client s3 = client;
        if (s3 != null)
            s3.close();
    }

    /** The key this file would have been uploaded under, or null if it is not an sstable component. */
    private String keyFor(File file)
    {
        try
        {
            Pair<Descriptor, Component> parsed = Descriptor.fromFileWithComponent(file);
            return S3ObjectKey.of(keyPrefix, parsed.left, parsed.right);
        }
        catch (Throwable t)
        {
            return null;
        }
    }
}
