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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;

import java.net.URI;

/**
 * Represents a provided replica being written.
 */
public class ProvidedReplicaBeingWritten extends ProvidedReplicaInPipeline {

  public ProvidedReplicaBeingWritten(long blockId, URI fileURI, long fileOffset,
      long len, long genStamp, FsVolumeSpi volume, Configuration conf,
      long bytesToReserve, Thread writer) {
    super(blockId, fileURI, fileOffset, len, genStamp, volume, conf,
        bytesToReserve, writer);
  }

  @Override
  public long getVisibleLength() {
    return getBytesAcked();       // all acked bytes are visible
  }

  @Override   //ReplicaInfo
  public HdfsServerConstants.ReplicaState getState() {
    return HdfsServerConstants.ReplicaState.RBW;
  }

  @Override  // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override  // Object
  public int hashCode() {
    return super.hashCode();
  }
}
