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

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.notifications.INotification;
import org.apache.cassandra.notifications.INotificationConsumer;
import org.apache.cassandra.notifications.SSTableAddedNotification;
import org.apache.cassandra.notifications.SSTableDeletingNotification;
import org.apache.cassandra.notifications.SSTableListChangedNotification;
import org.apache.cassandra.service.storage.StorageProviders;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

/**
 * Copies finished sstable components into S3, and removes them again when the sstable is dropped.
 * <p>
 * Both ways an sstable can come into existence are covered: a memtable flush, which arrives as
 * {@link SSTableAddedNotification}, and a lifecycle transaction producing new sstables from old ones -
 * compaction and friends - which arrives as {@link SSTableListChangedNotification}.
 * <p>
 * The write path itself is untouched: {@link org.apache.cassandra.io.util.SequentialWriter} builds the sstable on
 * local disk as it always has, with the ordinary fsync, and only once the lifecycle transaction has committed -
 * which is what {@link SSTableAddedNotification} means - does anything go to S3. So durability and crash
 * recovery keep working through the local file and the transaction log, and S3 holds a copy rather than the
 * authoritative state.
 * <p>
 * Uploads run on this class's own executor. They must not run on the caller's thread: notifications are
 * delivered on flush and compaction threads, and a flush blocked on a network round trip would back up
 * memtable space. The consequence is that an sstable is readable before its copy exists, which is why eviction
 * happens inside the upload task and never before it has been verified.
 */
class S3SSTableOffloader implements INotificationConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(S3SSTableOffloader.class);

    private final S3Client client;
    private final String bucket;
    private final String keyPrefix;
    private final Set<String> offloaded;
    private final S3Uploader uploader;
    private final ExecutorService executor;

    S3SSTableOffloader(S3Client client, String bucket, String keyPrefix, Set<String> offloaded, int threads)
    {
        this.client = client;
        this.bucket = bucket;
        this.keyPrefix = keyPrefix;
        this.offloaded = offloaded;
        this.uploader = new S3Uploader(client, bucket);
        this.executor = Executors.newFixedThreadPool(threads, threadFactory());
    }

    @Override
    public void handleNotification(INotification notification, Object sender)
    {
        if (notification instanceof SSTableAddedNotification)
        {
            // Deliberately not InitialSSTableAddedNotification: that fires for every sstable already on disk at
            // startup, and uploading the whole data set on restart is a decision for an operator, not a default.
            for (SSTableReader sstable : ((SSTableAddedNotification) notification).added)
                submit(sstable, this::offload, "upload");
        }
        else if (notification instanceof SSTableListChangedNotification)
        {
            // Anything that commits a LifecycleTransaction producing new sstables: compaction, cleanup, scrub,
            // upgradesstables. Only the additions are handled here - the sstables it replaced arrive separately
            // as SSTableDeletingNotification, once their last reference is released.
            for (SSTableReader sstable : ((SSTableListChangedNotification) notification).added)
                submit(sstable, this::offload, "upload");
        }
        else if (notification instanceof SSTableDeletingNotification)
        {
            submit(((SSTableDeletingNotification) notification).deleting, this::discard, "delete");
        }
    }

    private void submit(SSTableReader sstable, java.util.function.Consumer<SSTableReader> work, String what)
    {
        try
        {
            executor.execute(() -> {
                try
                {
                    work.accept(sstable);
                }
                catch (Throwable t)
                {
                    logger.error("Failed to {} {} in s3://{}", what, sstable.descriptor, bucket, t);
                }
            });
        }
        catch (Throwable t)
        {
            logger.error("Could not schedule {} of {}", what, sstable.descriptor, t);
        }
    }

    private void offload(SSTableReader sstable)
    {
        boolean evicted = false;

        for (Component component : sstable.getComponents())
        {
            if (!offloaded.contains(component.name()))
                continue;

            File file = sstable.descriptor.fileFor(component);
            if (!file.exists())
                continue; // already evicted, or never written for this format

            String key = S3ObjectKey.of(keyPrefix, sstable.descriptor, component);
            try
            {
                long bytes = uploader.upload(file, key);
                logger.debug("Uploaded {} bytes to s3://{}/{}", bytes, bucket, key);

                evict(file);
                evicted = true;
            }
            catch (Throwable t)
            {
                // The sstable stays wholly local; it is readable and durable, it just has no copy in S3 yet.
                logger.error("Failed to upload {} to s3://{}/{}", file, bucket, key, t);
            }
        }

        // Unlinking a component does not release it: the reader opened when this sstable was added still holds a
        // descriptor, so its blocks stay allocated and its reads keep coming off the unlinked inode. Replacing
        // the reader is what drops that descriptor, frees the space, and sends reads to the provider.
        if (evicted)
            StorageProviders.reopen(sstable);
    }

    /**
     * Drops the local copy once S3 has it, which is what makes this tiered storage rather than a backup: S3
     * becomes the only home of the component, and the read path in {@link S3FileChannel} becomes reachable.
     * <p>
     * Two consequences to know about. Readers already holding the file keep their open descriptor and carry on
     * reading the unlinked inode, so eviction does not affect queries already running against that sstable. And
     * when the sstable is eventually dropped, {@link org.apache.cassandra.db.lifecycle.LogTransaction#delete}
     * will not find this file and logs at ERROR - harmless, but noisy.
     */
    private void evict(File file)
    {
        try
        {
            file.delete();
            logger.debug("Evicted the local copy of {}", file);
        }
        catch (Throwable t)
        {
            logger.warn("Uploaded {} but could not evict the local copy", file, t);
        }
    }

    private void discard(SSTableReader sstable)
    {
        for (Component component : sstable.getComponents())
        {
            if (!offloaded.contains(component.name()))
                continue;

            String key = S3ObjectKey.of(keyPrefix, sstable.descriptor, component);
            // DeleteObject succeeds on a key that was never uploaded, so a failed upload needs no bookkeeping here.
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
            logger.debug("Deleted s3://{}/{}", bucket, key);
        }
    }

    void shutdown()
    {
        executor.shutdown();
        try
        {
            // Uploads in flight are worth waiting for: abandoning them leaves sstables with no copy in S3.
            if (!executor.awaitTermination(1, TimeUnit.MINUTES))
                logger.warn("Timed out waiting for s3 uploads to finish");
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory threadFactory()
    {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "S3Offload-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
