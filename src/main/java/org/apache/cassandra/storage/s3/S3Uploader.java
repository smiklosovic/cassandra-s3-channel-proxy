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
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.File;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

/**
 * Puts a local file into S3, as one PUT or as a multipart upload.
 * <p>
 * Multipart is not an optimisation here: a single PUT caps out at 5GiB and an sstable Data component can exceed
 * that, so anything above {@link #PART_SIZE} has to be parted whether or not it would benefit.
 */
class S3Uploader
{
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);

    /** Comfortably over S3's 5MiB minimum part size, and 10000 parts of it covers any plausible sstable. */
    static final long PART_SIZE = 16L * 1024 * 1024;

    private final S3Client client;
    private final String bucket;

    S3Uploader(S3Client client, String bucket)
    {
        this.client = client;
        this.bucket = bucket;
    }

    /**
     * Uploads {@code file} to {@code key} and verifies the stored length, returning the number of bytes uploaded.
     * The verification is what makes it safe for the caller to then drop the local copy.
     */
    long upload(File file, String key) throws IOException
    {
        long length = file.length();

        if (length <= PART_SIZE)
            putSingle(file, key);
        else
            putMultipart(file, key, length);

        long stored = client.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build())
                            .contentLength();
        if (stored != length)
            throw new IOException(String.format("Uploaded %s to s3://%s/%s but it stored %d bytes of %d",
                                                file, bucket, key, stored, length));

        return length;
    }

    private void putSingle(File file, String key)
    {
        client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                         RequestBody.fromFile(file.toPath()));
    }

    private void putMultipart(File file, String key, long length) throws IOException
    {
        String uploadId = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                                                                                   .bucket(bucket)
                                                                                   .key(key)
                                                                                   .build())
                                .uploadId();
        try
        {
            List<CompletedPart> parts = new ArrayList<>((int) (length / PART_SIZE) + 1);
            byte[] buffer = new byte[(int) PART_SIZE];

            try (InputStream in = java.nio.file.Files.newInputStream(file.toPath()))
            {
                for (int number = 1; ; number++)
                {
                    int read = readFully(in, buffer);
                    if (read == 0)
                        break;

                    UploadPartRequest request = UploadPartRequest.builder()
                                                                  .bucket(bucket)
                                                                  .key(key)
                                                                  .uploadId(uploadId)
                                                                  .partNumber(number)
                                                                  .build();

                    String etag = client.uploadPart(request, RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, read)))
                                        .eTag();
                    parts.add(CompletedPart.builder().partNumber(number).eTag(etag).build());

                    if (read < buffer.length)
                        break;
                }
            }

            client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                                                                          .bucket(bucket)
                                                                          .key(key)
                                                                          .uploadId(uploadId)
                                                                          .multipartUpload(CompletedMultipartUpload.builder()
                                                                                                                    .parts(parts)
                                                                                                                    .build())
                                                                          .build());
        }
        catch (Throwable t)
        {
            // An abandoned multipart upload is billed until aborted, so this is not merely tidiness.
            abortQuietly(key, uploadId);
            throw t;
        }
    }

    private void abortQuietly(String key, String uploadId)
    {
        try
        {
            client.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                                                                    .bucket(bucket)
                                                                    .key(key)
                                                                    .uploadId(uploadId)
                                                                    .build());
        }
        catch (Throwable t)
        {
            logger.warn("Could not abort multipart upload {} of s3://{}/{}; it will be billed until it expires or " +
                        "a lifecycle rule removes it", uploadId, bucket, key, t);
        }
    }

    /** Parts other than the last must be exactly PART_SIZE, so a short read from the stream cannot be passed on. */
    private static int readFully(InputStream in, byte[] buffer) throws IOException
    {
        int total = 0;
        while (total < buffer.length)
        {
            int read = in.read(buffer, total, buffer.length - total);
            if (read < 0)
                break;
            total += read;
        }
        return total;
    }
}
