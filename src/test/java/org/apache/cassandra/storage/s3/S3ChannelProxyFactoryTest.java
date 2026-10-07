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

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.storage.s3.S3ChannelProxyFactory.components;
import static org.apache.cassandra.storage.s3.S3ChannelProxyFactory.normalizePrefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Option parsing only. Anything touching Descriptor or ChannelProxy needs Cassandra's transitive dependencies,
 * which this project deliberately does not put on its classpath; that belongs in an in-JVM dtest on the node side.
 */
class S3ChannelProxyFactoryTest
{
    @Test
    void prefixGainsOneTrailingSeparator()
    {
        assertEquals("backup/", normalizePrefix("backup"));
        assertEquals("backup/", normalizePrefix("backup/"));
        assertEquals("a/b/", normalizePrefix("a/b"));
    }

    /** A prefix is a key namespace, and a key starting with "/" names an object whose first path segment is empty. */
    @Test
    void prefixLosesLeadingSeparators()
    {
        assertEquals("backup/", normalizePrefix("/backup"));
        assertEquals("backup/", normalizePrefix("///backup/"));
    }

    @Test
    void absentPrefixIsEmpty()
    {
        assertEquals("", normalizePrefix(null));
        assertEquals("", normalizePrefix(""));
        assertEquals("", normalizePrefix("   "));
        assertEquals("", normalizePrefix("/"));
    }

    @Test
    void componentsAreSplitAndTrimmed()
    {
        assertEquals(Arrays.asList("Data.db", "Index.db"), new java.util.ArrayList<>(components("Data.db, Index.db")));
        assertEquals(Arrays.asList("Data.db"), new java.util.ArrayList<>(components(" Data.db ")));
    }

    @Test
    void componentsIgnoreBlankEntries()
    {
        assertEquals(Arrays.asList("Data.db"), new java.util.ArrayList<>(components("Data.db,,  ,")));
        assertTrue(components(",, ").isEmpty());
    }
}
