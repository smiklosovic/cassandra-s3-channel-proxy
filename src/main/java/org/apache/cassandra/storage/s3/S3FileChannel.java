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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.NoSuchFileException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * A read-only {@link FileChannel} over a single S3 object.
 * <p>
 * Only positional reads are supported, because only they have an honest mapping: {@code read(dst, position)}
 * becomes a ranged GET. Everything else is rejected rather than emulated, so an unsupported path fails loudly at
 * the call site instead of quietly costing a full object download.
 * <p>
 * In particular {@link #map} cannot work - there is nothing to memory-map - which is why a node using this
 * provider must run with {@code disk_access_mode: standard}.
 */
public class S3FileChannel extends FileChannel
{
    private static final Logger logger = LoggerFactory.getLogger(S3FileChannel.class);

    /** How S3 reports a range starting at or past the end of an object. */
    private static final int RANGE_NOT_SATISFIABLE = 416;

    private static final long UNKNOWN_SIZE = -1;

    /** Staging array for direct buffers, which cannot be filled from an InputStream directly. */
    private static final int COPY_CHUNK = 64 * 1024;

    private final S3Client client;
    private final String bucket;
    private final String key;

    /**
     * Object length, or {@link #UNKNOWN_SIZE} until first learnt. Safe to cache for the channel's lifetime
     * because sstable components are immutable once written.
     */
    private volatile long size = UNKNOWN_SIZE;

    /** So that the first read of an object says so once, rather than every read saying it again. */
    private volatile boolean announced;

    public S3FileChannel(S3Client client, String bucket, String key)
    {
        this.client = client;
        this.bucket = bucket;
        this.key = key;
    }

    /**
     * The one operation that maps cleanly: a ranged GET. One request per call - there is no read-ahead here
     * because Cassandra already does it a layer up, in the chunk cache and the rebufferer.
     */
    @Override
    public int read(ByteBuffer dst, long position) throws IOException
    {
        if (position < 0)
            throw new IllegalArgumentException("Negative position: " + position);

        ensureOpen();

        int wanted = dst.remaining();
        if (wanted == 0)
            return 0;

        GetObjectRequest request = GetObjectRequest.builder()
                                                   .bucket(bucket)
                                                   .key(key)
                                                   .range("bytes=" + position + '-' + (position + wanted - 1))
                                                   .build();

        if (!announced)
        {
            announced = true;
            logger.debug("First read of s3://{}/{}; this component is being served from the provider", bucket, key);
        }

        logger.trace("GetObject s3://{}/{} range {}-{}", bucket, key, position, position + wanted - 1);

        try (ResponseInputStream<GetObjectResponse> in = client.getObject(request))
        {
            cacheSizeFrom(in.response().contentRange());
            int read = transfer(in, dst);
            // A ranged GET that yields nothing means the range began at EOF, which FileChannel reports as -1.
            return read == 0 ? -1 : read;
        }
        catch (NoSuchKeyException e)
        {
            throw new NoSuchFileException(uri());
        }
        catch (S3Exception e)
        {
            // Reading at or past the end is legal for a FileChannel caller and must look like EOF, not failure.
            if (e.statusCode() == RANGE_NOT_SATISFIABLE)
                return -1;
            throw new IOException("Failed to read " + uri() + " at " + position, e);
        }
        catch (SdkException e)
        {
            throw new IOException("Failed to read " + uri() + " at " + position, e);
        }
    }

    /**
     * HeadObject, cached. Callers treat size() as free - it costs a syscall on a local file - whereas here it is
     * a network round trip, and {@link org.apache.cassandra.io.util.ChannelProxy#size()} is on hot paths.
     */
    @Override
    public long size() throws IOException
    {
        long known = size;
        if (known != UNKNOWN_SIZE)
            return known;

        ensureOpen();

        try
        {
            long length = client.headObject(HeadObjectRequest.builder()
                                                             .bucket(bucket)
                                                             .key(key)
                                                             .build())
                                .contentLength();
            size = length;
            return length;
        }
        catch (NoSuchKeyException e)
        {
            throw new NoSuchFileException(uri());
        }
        catch (SdkException e)
        {
            throw new IOException("Failed to determine the size of " + uri(), e);
        }
    }

    /**
     * A ranged GET already reports the object's full length in Content-Range ("bytes 0-99/12345"), so a channel
     * that has served one read never needs a HeadObject to answer {@link #size()}.
     */
    private void cacheSizeFrom(String contentRange)
    {
        if (size != UNKNOWN_SIZE || contentRange == null)
            return;

        int slash = contentRange.lastIndexOf('/');
        if (slash < 0)
            return;

        try
        {
            size = Long.parseLong(contentRange.substring(slash + 1).trim());
        }
        catch (NumberFormatException e)
        {
            // "bytes 0-99/*": the total is not always disclosed, so leave it to size()
        }
    }

    /**
     * Fills {@code dst} from the response body, returning the number of bytes that arrived - fewer than
     * requested when the range ran past the end of the object. Cassandra's buffers are usually direct, which is
     * the branch needing the staging array; the heap branch exists to skip that copy where it can.
     */
    static int transfer(InputStream in, ByteBuffer dst) throws IOException
    {
        int wanted = dst.remaining();
        int total = 0;

        if (dst.hasArray() && !dst.isReadOnly())
        {
            byte[] array = dst.array();
            int base = dst.arrayOffset() + dst.position();
            while (total < wanted)
            {
                int read = in.read(array, base + total, wanted - total);
                if (read < 0)
                    break;
                total += read;
            }
            dst.position(dst.position() + total);
        }
        else
        {
            byte[] chunk = new byte[Math.min(wanted, COPY_CHUNK)];
            while (total < wanted)
            {
                int read = in.read(chunk, 0, Math.min(chunk.length, wanted - total));
                if (read < 0)
                    break;
                dst.put(chunk, 0, read);
                total += read;
            }
        }

        return total;
    }

    private void ensureOpen() throws ClosedChannelException
    {
        if (!isOpen())
            throw new ClosedChannelException();
    }

    private String uri()
    {
        return "s3://" + bucket + '/' + key;
    }

    @Override
    protected void implCloseChannel()
    {
        // the SDK client is shared and owned by the factory; nothing per-object to release
    }

    // ---- no honest implementation over an object store ----

    /** Memory mapping requires a page-cacheable device. Forces disk_access_mode: standard. */
    @Override
    public MappedByteBuffer map(MapMode mode, long position, long size)
    {
        throw new UnsupportedOperationException("S3-backed sstables cannot be mmapped; use disk_access_mode: standard");
    }

    /** Zero-copy sendfile has no S3 equivalent; a real implementation would have to buffer through the heap. */
    @Override
    public long transferTo(long position, long count, WritableByteChannel target)
    {
        throw new UnsupportedOperationException("transferTo is not supported by the s3 provider");
    }

    // ---- read-only, and stateful-cursor operations are deliberately absent ----

    @Override public int read(ByteBuffer dst) { throw new UnsupportedOperationException("positional reads only"); }
    @Override public long read(ByteBuffer[] dsts, int offset, int length) { throw new UnsupportedOperationException("positional reads only"); }
    @Override public long position() { throw new UnsupportedOperationException("positional reads only"); }
    @Override public FileChannel position(long newPosition) { throw new UnsupportedOperationException("positional reads only"); }

    @Override public int write(ByteBuffer src) { throw new UnsupportedOperationException("read-only"); }
    @Override public long write(ByteBuffer[] srcs, int offset, int length) { throw new UnsupportedOperationException("read-only"); }
    @Override public int write(ByteBuffer src, long position) { throw new UnsupportedOperationException("read-only"); }
    @Override public FileChannel truncate(long size) { throw new UnsupportedOperationException("read-only"); }
    @Override public void force(boolean metaData) { throw new UnsupportedOperationException("read-only"); }
    @Override public long transferFrom(ReadableByteChannel src, long position, long count) { throw new UnsupportedOperationException("read-only"); }

    @Override public FileLock lock(long position, long size, boolean shared) { throw new UnsupportedOperationException("no file locking on s3"); }
    @Override public FileLock tryLock(long position, long size, boolean shared) { throw new UnsupportedOperationException("no file locking on s3"); }
}
