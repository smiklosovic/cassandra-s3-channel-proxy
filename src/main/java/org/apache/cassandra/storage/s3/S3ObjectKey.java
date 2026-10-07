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

import org.apache.cassandra.db.Directories;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;

/**
 * Maps an sstable component to an S3 object key.
 * <p>
 * Both directions depend on this agreeing with itself: the read path derives a key from a {@link File} it was
 * asked to open, the write path derives one from the {@link Descriptor} of an sstable it is uploading, and a
 * mismatch shows up as a missing object rather than as an error. Hence one class, used by both.
 * <p>
 * Keys are deliberately not the local absolute path. The path contains the data directory, which differs between
 * nodes and changes when disks are added, whereas the table directory carries the table id and so survives a
 * drop and recreate of a table with the same name.
 */
class S3ObjectKey
{
    private S3ObjectKey() { }

    static String of(String prefix, Descriptor descriptor, Component component)
    {
        return prefix + descriptor.ksname + '/' + tableDirectory(descriptor.directory) + '/'
               + descriptor.fileFor(component).name();
    }

    /**
     * The table directory ("tbl-<table id>"), or "tbl-<table id>/.index" for a secondary index, whose own folder
     * name is only unique within its table.
     */
    private static String tableDirectory(File directory)
    {
        return Directories.isSecondaryIndexFolder(directory) ? directory.parent().name() + '/' + directory.name()
                                                             : directory.name();
    }
}
