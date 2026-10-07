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

import java.net.URI;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** A local S3 in docker, so the end-to-end tests exercise real HTTP against a real S3 API. */
class S3Mock extends GenericContainer<S3Mock>
{
    private static final int PORT = 9090;

    static final String ACCESS_KEY = "test";
    static final String SECRET_KEY = "test";
    static final String REGION = "us-east-1";

    S3Mock()
    {
        super(DockerImageName.parse("adobe/s3mock:4.3.0"));
        withExposedPorts(PORT);
        withEnv("retainFilesOnExit", "false");
        withEnv("debug", "false");
    }

    String endpoint()
    {
        return "http://" + getHost() + ':' + getMappedPort(PORT);
    }

    /**
     * A client for the test's own assertions. Path style is required: the endpoint is reached by host and port,
     * so a bucket cannot be a DNS label in front of it.
     */
    S3Client client()
    {
        return S3Client.builder()
                       .endpointOverride(URI.create(endpoint()))
                       .region(Region.of(REGION))
                       .forcePathStyle(true)
                       .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY,
                                                                                                         SECRET_KEY)))
                       .build();
    }
}
