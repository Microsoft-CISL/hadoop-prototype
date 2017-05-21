/**
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

package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * This class is a stub for reading file regions from the block map.
 */
public class FileRegionProvider implements Iterable<FileRegion> {
  @Override
  public Iterator<FileRegion> iterator() {
    return Collections.emptyListIterator();
  }

  public void refresh() throws IOException {
    return;
  }

  public boolean finalize(FileRegion region) throws IOException {
    //expects to be overridden!
    return false;
  }
}