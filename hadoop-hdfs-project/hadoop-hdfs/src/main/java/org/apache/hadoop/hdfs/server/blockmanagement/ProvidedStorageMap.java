package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.util.ReflectionUtils;

import com.google.protobuf.ByteString;

public class ProvidedStorageMap {

  public static final String FMT = "hdfs.namenode.block.provider.class";
  public static final String STORAGE_ID = "hdfs.namenode.block.provider.id";

  // limit to a single provider for now
  private final BlockProvider p;
  private final String storageId;
  private volatile DatanodeStorageInfo storage;

  ProvidedStorageMap(RwLock lock, BlockManager bm, Configuration conf)
      throws IOException {
    storageId = conf.get(STORAGE_ID);
    if (null == storageId) {
      // disable mapping
      p = null;
      storage = null;
      return;
    }
    //State.READ_ONLY_SHARED ?
    DatanodeStorage ds =
      new DatanodeStorage(storageId, State.NORMAL, StorageType.PROVIDED);
    // TODO need a subclass for DNDesc, DNSI, or both
    storage = new DatanodeStorageInfo(null, ds);

    // load block reader into storage
    Class<? extends BlockProvider> fmt =
      conf.getClass(FMT, BlockProvider.class, BlockProvider.class);
    p = ReflectionUtils.newInstance(fmt, conf);
    p.init(lock, bm, storage);
    p.start();
  }

  // TODO: need to capture DN registration w/ storage
  //       possibly snare from getStorage

  DatanodeStorageInfo getStorage(DatanodeDescriptor dn, DatanodeStorage s) {
    if (storageId != null && storageId.equals(s.getStorageID())) {
      return storage;
    }
    return dn.getStorageInfo(s.getStorageID());
  }

  static class ProvidedBlockList extends BlockListAsLongs {

    final Iterator<Block> inner;

    ProvidedBlockList(Iterator<Block> inner) {
      this.inner = inner;
    }

    @Override
    public Iterator<BlockReportReplica> iterator() {
      return new Iterator<BlockReportReplica>() {
        @Override
        public BlockReportReplica next() {
          return new BlockReportReplica(inner.next());
        }
        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
      };
    }

    @Override
    public int getNumberOfBlocks() {
      // VERIFY: only printed for debugging
      return -1;
    }

    @Override
    public ByteString getBlocksBuffer() {
      throw new UnsupportedOperationException();
      //Builder builder = builder();
      //for (Replica replica : this) {
      //  builder.add(replica);
      //}
      //return builder.build().getBlocksBuffer();
    }

    @Override
    public long[] getBlockListAsLongs() {
      // should only be used for backwards compat, DN.ver > NN.ver
      throw new UnsupportedOperationException();
    }

  }

  // use common Service framework from common? No instances in HDFS, yet
  public static class BlockProvider implements Iterable<Block> {

    RwLock lock;
    Executor exec;
    BlockManager bm;
    DatanodeStorageInfo storage;

    void init(RwLock lock, BlockManager bm, DatanodeStorageInfo storage) {
      this.bm = bm;
      this.lock = lock;
      this.storage = storage;
      // set ThreadFactory to return daemon thread
      exec = Executors.newSingleThreadExecutor();
    }

    void start() {
      // create background thread to read from fmt
      // forall blocks
      //   batch ProvidedBlockList from BlockProvider
      //   report BM::processFirstBlockReport
      //   subsequently report BM::processReport
      FutureTask<Boolean> t = new FutureTask<>(new RefreshBlocks());
      exec.execute(t);
      // TODO: report the result at some point
    }

    class RefreshBlocks implements Callable<Boolean> {

      @Override
      public Boolean call() {
        lock.writeLock();
        try {
          // first pass; periodic refresh should call bm.processReport
          bm.processFirstBlockReport(storage, new ProvidedBlockList(iterator()));
          return true;
        } catch (IOException e) {
          return false;
        } finally {
          lock.writeUnlock();
        }
      }

    }

    @Override
    public Iterator<Block> iterator() {
      // default to empty storage
      return Collections.emptyListIterator();
    }

  }

}
