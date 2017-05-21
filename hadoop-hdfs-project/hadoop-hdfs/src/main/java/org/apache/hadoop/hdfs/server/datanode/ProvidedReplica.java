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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi.ScanInfo;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class is used as a base class for provided replicas.
 */
public abstract class ProvidedReplica extends ReplicaInfo {

  public static final Logger LOG =
      LoggerFactory.getLogger(ProvidedReplica.class);

  // Null checksum information for provided replicas.
  // Shared across all replicas.
  static final byte[] NULL_CHECKSUM_ARRAY =
      FsDatasetUtil.createNullChecksumByteArray();
  private URI fileURI;
  private long fileOffset;
  private Configuration conf;
  private FileSystem remoteFS;

  /**
   * Constructor.
   * @param blockId block id
   * @param fileURI remote URI this block is to be read from
   * @param fileOffset the offset in the remote URI
   * @param blockLen the length of the block
   * @param genStamp the generation stamp of the block
   * @param volume the volume this block belongs to
   */
  public ProvidedReplica(long blockId, URI fileURI, long fileOffset,
      long blockLen, long genStamp, FsVolumeSpi volume, Configuration conf) {
    super(volume, blockId, blockLen, genStamp);
    this.fileURI = fileURI;
    this.fileOffset = fileOffset;
    this.conf = conf;
    try {
      this.remoteFS = FileSystem.get(fileURI, this.conf);
    } catch (IOException e) {
      LOG.warn("Failed to obtain filesystem for " + fileURI);
      this.remoteFS = null;
    }
  }

  public ProvidedReplica(ProvidedReplica r) {
    super(r);
    this.fileURI = r.fileURI;
    this.fileOffset = r.fileOffset;
    this.conf = r.conf;
    try {
      this.remoteFS = FileSystem.newInstance(fileURI, this.conf);
    } catch (IOException e) {
      this.remoteFS = null;
    }
  }

  @Override
  public URI getBlockURI() {
    return this.fileURI;
  }

  @Override
  public InputStream getDataInputStream(long seekOffset) throws IOException {
    if (remoteFS != null) {
      FSDataInputStream ins = remoteFS.open(new Path(fileURI));
      ins.seek(fileOffset + seekOffset);
      return new BoundedInputStream(
          new FSDataInputStream(ins), getBlockDataLength());
    } else {
      throw new IOException("Remote filesystem for provided replica " + this +
          " does not exist");
    }
  }

  @Override
  public OutputStream getDataOutputStream(boolean append) throws IOException {
    throw new UnsupportedOperationException(
        "OutputDataStream is not implemented for ProvidedReplica");
  }

  @Override
  public URI getMetadataURI() {
    return null;
  }

  @Override
  public OutputStream getMetadataOutputStream(boolean append)
      throws IOException {
    return null;
  }

  @Override
  public boolean blockDataExists() {
    if(remoteFS != null) {
      try {
        return remoteFS.exists(new Path(fileURI));
      } catch (IOException e) {
        return false;
      }
    } else {
      return false;
    }
  }

  @Override
  public boolean deleteBlockData() {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support deleting block data");
  }

  @Override
  public long getBlockDataLength() {
    return this.getNumBytes();
  }

  @Override
  public LengthInputStream getMetadataInputStream(long offset)
      throws IOException {
    return new LengthInputStream(new ByteArrayInputStream(NULL_CHECKSUM_ARRAY),
        NULL_CHECKSUM_ARRAY.length);
  }

  @Override
  public boolean metadataExists() {
    return NULL_CHECKSUM_ARRAY == null ? false : true;
  }

  @Override
  public boolean deleteMetadata() {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support deleting metadata");
  }

  @Override
  public long getMetadataLength() {
    return NULL_CHECKSUM_ARRAY == null ? 0 : NULL_CHECKSUM_ARRAY.length;
  }

  @Override
  public boolean renameMeta(URI destURI) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support renaming metadata");
  }

  @Override
  public boolean renameData(URI destURI) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support renaming data");
  }

  @Override
  public boolean getPinning(LocalFileSystem localFS) throws IOException {
    return false;
  }

  @Override
  public void setPinning(LocalFileSystem localFS) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not support pinning");
  }

  @Override
  public void bumpReplicaGS(long newGS) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support writes");
  }

  @Override
  public boolean breakHardLinksIfNeeded() throws IOException {
    return false;
  }

  @Override
  public ReplicaRecoveryInfo createInfo()
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support writes");
  }

  @Override
  public int compareWith(ScanInfo info) {
    //local scanning cannot find any provided blocks.
    if (info.getFileRegion().equals(
        new FileRegion(this.getBlockId(), new Path(fileURI),
            fileOffset, this.getNumBytes(), this.getGenerationStamp()))) {
      return 0;
    } else {
      return (int) (info.getBlockLength() - getNumBytes());
    }
  }

  @Override
  public void truncateBlock(long newLength) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support truncate");
  }

  @Override
  public void updateWithReplica(StorageLocation replicaLocation) {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support update");
  }

  @Override
  public void copyMetadata(URI destination) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support copy metadata");
  }

  @Override
  public void copyBlockdata(URI destination) throws IOException {
    throw new UnsupportedOperationException(
        "ProvidedReplica does not yet support copy data");
  }
  
  public long getOffset() {
    return fileOffset;
  }

  @Override
  public boolean isProvided() {
    return true;
  }
}
