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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.storage.s3.S3FileChannel.transfer;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Covers {@link S3FileChannel#transfer} only. The S3 calls around it need a mock endpoint; the buffer
 * arithmetic here does not, and is where this code is most likely to be wrong.
 */
class S3FileChannelTest
{
    private static final byte[] DATA = bytes(0, 16);

    private static byte[] bytes(int from, int count)
    {
        byte[] out = new byte[count];
        for (int i = 0; i < count; i++)
            out[i] = (byte) (from + i);
        return out;
    }

    private static byte[] drain(ByteBuffer buffer, int from, int count)
    {
        byte[] out = new byte[count];
        buffer.duplicate().position(from).get(out);
        return out;
    }

    @Test
    void heapBuffer() throws IOException
    {
        ByteBuffer dst = ByteBuffer.allocate(16);
        assertEquals(16, transfer(new ByteArrayInputStream(DATA), dst));
        assertEquals(16, dst.position());
        assertArrayEquals(DATA, dst.array());
    }

    @Test
    void directBuffer() throws IOException
    {
        ByteBuffer dst = ByteBuffer.allocateDirect(16);
        assertEquals(16, transfer(new ByteArrayInputStream(DATA), dst));
        assertEquals(16, dst.position());
        assertArrayEquals(DATA, drain(dst, 0, 16));
    }

    /** A sliced heap buffer has a non-zero arrayOffset, so writing at array[position] would corrupt it. */
    @Test
    void slicedHeapBuffer() throws IOException
    {
        ByteBuffer backing = ByteBuffer.allocate(24);
        backing.position(4);
        ByteBuffer dst = backing.slice();
        dst.limit(16);

        assertEquals(16, transfer(new ByteArrayInputStream(DATA), dst));
        assertEquals(16, dst.position());
        assertArrayEquals(DATA, drain(backing, 4, 16));
        assertArrayEquals(new byte[4], drain(backing, 0, 4));
    }

    /** Writing into the middle of a buffer must not disturb what is already there. */
    @Test
    void respectsExistingPosition() throws IOException
    {
        ByteBuffer dst = ByteBuffer.allocate(24);
        dst.put(bytes(100, 4));

        assertEquals(16, transfer(new ByteArrayInputStream(DATA), dst));
        assertEquals(20, dst.position());
        assertArrayEquals(bytes(100, 4), drain(dst, 0, 4));
        assertArrayEquals(DATA, drain(dst, 4, 16));
    }

    /** A range running past the end of the object yields a short read, not an error. */
    @Test
    void shortRead() throws IOException
    {
        ByteBuffer dst = ByteBuffer.allocateDirect(16);
        assertEquals(6, transfer(new ByteArrayInputStream(bytes(0, 6)), dst));
        assertEquals(6, dst.position());
        assertArrayEquals(bytes(0, 6), drain(dst, 0, 6));
    }

    @Test
    void atEof() throws IOException
    {
        ByteBuffer dst = ByteBuffer.allocate(16);
        assertEquals(0, transfer(new ByteArrayInputStream(new byte[0]), dst));
        assertEquals(0, dst.position());
    }

    @Test
    void fullBufferIssuesNoRead() throws IOException
    {
        ByteBuffer dst = ByteBuffer.allocate(16);
        dst.position(16);
        assertEquals(0, transfer(failingStream(), dst));
    }

    /** A stream handing back one byte at a time must still fill the buffer. */
    @Test
    void dripFedStream() throws IOException
    {
        for (ByteBuffer dst : new ByteBuffer[]{ ByteBuffer.allocate(16), ByteBuffer.allocateDirect(16) })
        {
            assertEquals(16, transfer(oneByteAtATime(DATA), dst));
            assertArrayEquals(DATA, drain(dst, 0, 16));
        }
    }

    /** Larger than COPY_CHUNK, so the direct path has to loop over its staging array. */
    @Test
    void spansMultipleChunks() throws IOException
    {
        byte[] data = bytes(0, 200 * 1024);
        ByteBuffer dst = ByteBuffer.allocateDirect(data.length);

        assertEquals(data.length, transfer(new ByteArrayInputStream(data), dst));
        assertArrayEquals(data, drain(dst, 0, data.length));
    }

    private static InputStream oneByteAtATime(byte[] data)
    {
        return new ByteArrayInputStream(data)
        {
            @Override
            public synchronized int read(byte[] b, int off, int len)
            {
                return super.read(b, off, Math.min(len, 1));
            }
        };
    }

    private static InputStream failingStream()
    {
        return new InputStream()
        {
            @Override
            public int read()
            {
                throw new AssertionError("must not read into a full buffer");
            }

            @Override
            public int read(byte[] b, int off, int len)
            {
                throw new AssertionError("must not read into a full buffer");
            }
        };
    }
}
