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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.hdfs.server.protocol.ReplicaRecoveryInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class defines a provided replica that is in pipeline, which includes
 * replica currently being written by a client or a temporary replica being
 * written by a datanode.
 *
 * The base class implements a temporary replica
 * TODO - lot of code repeated from LocalReplicaInPipeline -- consider subclassing it
 */
public class ProvidedReplicaInPipeline extends ProvidedReplica
    implements ReplicaInPipeline {

  private long bytesAcked;
  private long bytesOnDisk;
  private byte[] lastChecksum;
  private AtomicReference<Thread> writer = new AtomicReference<Thread>();

  /**
   * Bytes reserved for this replica on the containing volume.
   * Based off difference between the estimated maximum block length and
   * the bytes already written to this block.
   */
  private long bytesReserved;
  private final long originalBytesReserved;

  private Configuration conf;

  public ProvidedReplicaInPipeline(long blockId, URI fileURI, long fileOffset,
      long genStamp, FsVolumeSpi volume, Configuration conf,
      long bytesToReserve) {
    this(blockId, fileURI, fileOffset, 0L, genStamp, volume, conf,
        bytesToReserve, Thread.currentThread());
  }

  public ProvidedReplicaInPipeline(long blockId, URI fileURI, long fileOffset,
      long genStamp, FsVolumeSpi volume, Configuration conf,
      long bytesToReserve, Thread writer) {
    this(blockId, fileURI, fileOffset, 0L, genStamp, volume, conf,
        bytesToReserve, writer);
  }

  public ProvidedReplicaInPipeline(long blockId, URI fileURI, long fileOffset,
      long len, long genStamp, FsVolumeSpi volume, Configuration conf,
      long bytesToReserve, Thread writer) {
    super(blockId, fileURI, fileOffset, len, genStamp, volume, conf);
    this.bytesAcked = len;
    this.bytesOnDisk = len;
    this.originalBytesReserved = bytesToReserve;
    this.writer.set(writer);
    this.conf = conf;
  }

  @Override
  public ReplicaState getState() {
    return ReplicaState.TEMPORARY;
  }

  @Override
  public long getBytesOnDisk() {
    return bytesOnDisk;
  }

  @Override
  public long getVisibleLength() {
    return -1;
  }

  @Override
  public long getBytesReserved() {
    return bytesReserved;
  }

  @Override
  public long getOriginalBytesReserved() {
    return originalBytesReserved;
  }

  @Override
  public ReplicaInfo getOriginalReplica() {
    throw new UnsupportedOperationException("Replica of type " + getState()
        + " does not support getOriginalReplica");
  }

  @Override
  public long getRecoveryID() {
    throw new UnsupportedOperationException(
        "Replica of type " + getState() + " does not support getRecoveryID");
  }

  @Override
  public void setRecoveryID(long recoveryId) {
    throw new UnsupportedOperationException(
        "Replica of type " + getState() + " does not support setRecoveryID");
  }

  @Override
  public ReplicaRecoveryInfo createInfo() {
    throw new UnsupportedOperationException(
        "Replica of type " + getState() + " does not support createInfo");
  }

  @Override // ReplicaInPipeline
  public long getBytesAcked() {
    return bytesAcked;
  }

  @Override // ReplicaInPipeline
  public void setBytesAcked(long bytesAcked) {
    long newBytesAcked = bytesAcked - this.bytesAcked;
    this.bytesAcked = bytesAcked;

    // Once bytes are ACK'ed we can release equivalent space from the
    // volume's reservedForRbw count. We could have released it as soon
    // as the write-to-disk completed but that would be inefficient.
    getVolume().releaseReservedSpace(newBytesAcked);
    bytesReserved -= newBytesAcked;
  }

  @Override // ReplicaInPipeline
  public void releaseAllBytesReserved() {
    getVolume().releaseReservedSpace(bytesReserved);
    getVolume().releaseLockedMemory(bytesReserved);
    bytesReserved = 0;
  }

  @Override // ReplicaInPipeline
  public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum) {
    this.bytesOnDisk = dataLength;
    this.lastChecksum = lastChecksum;
  }

  @Override // ReplicaInPipeline
  public ChunkChecksum getLastChecksumAndDataLen() {
    return new ChunkChecksum(getBytesOnDisk(), lastChecksum);
  }

  public class ProvidedOutputStream extends OutputStream {

    private FSDataOutputStream out;
    private Path newPath;
    private Path originalPath;

    private boolean supportsAppend;
    public ProvidedOutputStream(Path originalPath) throws IOException {
      FileSystem remoteFS = FileSystem.newInstance(getBlockURI(), conf);
      if (remoteFS instanceof org.apache.hadoop.fs.LocalFileSystem) {
        remoteFS = ((LocalFileSystem)remoteFS).getRaw();
      }
      try {
         this.out = remoteFS.append(originalPath);
      } catch (UnsupportedOperationException e) {
        supportsAppend = false;
        //TODO what do we do if append is not supported!
      }
      this.originalPath = originalPath;
    }

    @Override
    public void write(int i) throws IOException {
      int[] bytes = {i};
      out.write(i);
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void write(byte[] b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }


    @Override
    public void close() throws IOException {
      if (supportsAppend) {
        out.close();
      } else {
        //TODO recover when append is not supported.
      }
    }
  }

  @Override // ReplicaInPipeline
  public ReplicaOutputStreams createStreams(boolean isCreate,
      DataChecksum requestedChecksum) throws IOException {

    // the checksum that should actually be used -- this
    // may differ from requestedChecksum for appends.
    final DataChecksum checksum;

    if (!isCreate) {
      // TODO For append or recovery of block
      checksum = requestedChecksum;
    } else {
      // for create, we can use the requested checksum
      checksum = requestedChecksum;
    }

    OutputStream blockOut = null;
    OutputStream crcOut = null;
    try {
      FileSystem remoteFS = FileSystem.newInstance(getBlockURI(), conf);
      //TODO fileOffset is not required if we use append here!
      //TODO have to do an append here!!
      Path existingPath = new Path(getBlockURI());
      if (remoteFS.exists(existingPath)) {
        blockOut = new ProvidedOutputStream(existingPath);
      } else {
        blockOut = remoteFS.create(new Path(getBlockURI()));
      }
      crcOut = remoteFS.create(new Path(getBlockURI().getPath() + ".meta." + getBlockId()));
      if (!isCreate) {
        // TODO For append or recovery of block
      }
      return new ReplicaOutputStreams(blockOut, crcOut, checksum,
              getVolume(), null);
    } catch (IOException e) {
      IOUtils.closeStream(blockOut);
      IOUtils.closeStream(crcOut);
      throw e;
    }
  }

  @Override // ReplicaInPipeline
  public OutputStream createRestartMetaStream() throws IOException {
    FileSystem remoteFS = FileSystem.newInstance(getBlockURI(), conf);
    return remoteFS.create(new Path(
        getBlockURI().getPath() + ".meta." + getBlockId() + ".restart"));
  }

  @Override // ReplicaInPipeline
  public ReplicaInfo getReplicaInfo() {
    return this;
  }

  @Override // ReplicaInPipeline
  public void setWriter(Thread writer) {
    this.writer.set(writer);
  }

  @Override
  public void interruptThread() {
    Thread thread = writer.get();
    if (thread != null && thread != Thread.currentThread()
            && thread.isAlive()) {
      thread.interrupt();
    }
  }

  @Override // Object
  public boolean equals(Object o) {
    return super.equals(o);
  }

  /**
   * Attempt to set the writer to a new value.
   */
  @Override // ReplicaInPipeline
  public boolean attemptToSetWriter(Thread prevWriter, Thread newWriter) {
    return writer.compareAndSet(prevWriter, newWriter);
  }

  /**
   * Interrupt the writing thread and wait until it dies.
   *
   * @throws IOException the waiting is interrupted
   */
  @Override // ReplicaInPipeline
  public void stopWriter(long xceiverStopTimeout) throws IOException {
    while (true) {
      Thread thread = writer.get();
      if ((thread == null) || (thread == Thread.currentThread())
          || (!thread.isAlive())) {
        if (writer.compareAndSet(thread, null)) {
          return; // Done
        }
        // The writer changed. Go back to the start of the loop and attempt to
        // stop the new writer.
        continue;
      }
      thread.interrupt();
      try {
        thread.join(xceiverStopTimeout);
        if (thread.isAlive()) {
          // Our thread join timed out.
          final String msg = "Join on writer thread " + thread + " timed out";
          DataNode.LOG.warn(msg + "\n" + StringUtils.getStackTrace(thread));
          throw new IOException(msg);
        }
      } catch (InterruptedException e) {
        throw new IOException("Waiting for writer thread is interrupted.");
      }
    }
  }

}
