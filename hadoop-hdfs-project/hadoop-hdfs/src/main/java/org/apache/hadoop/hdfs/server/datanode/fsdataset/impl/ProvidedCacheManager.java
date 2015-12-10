package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ProvidedCacheManager {

  private FsDatasetImpl dataset;
  protected ThreadPoolExecutor cacheExecutor;
  
  private static final class Value {
    final State state;
    final StorageType storageType;

    Value(State state, StorageType storageType) {
      this.storageType = storageType;
      this.state = state;
    }
  }

  
  private enum State {
    /**
     * A block being cached.
     */
    CACHING,

    /**
     * The block was in the process of being cached, but it was
     * cancelled.  
     */
    CACHING_CANCELLED,

    /**
     * The block has been cached 
     */
    CACHED,

    /**
     * The block is being uncached
     */
    UNCACHING;
  }
  
  HashMap<ExtendedBlock, Value>  blocksForCaching = new HashMap<ExtendedBlock, Value>();
  
  private static final Logger LOG = LoggerFactory.getLogger(FsDatasetCache
      .class);
  
  private class CachingTask implements Runnable {

    ExtendedBlock blockToCache;
    StorageType storageType;
    
    CachingTask(ExtendedBlock blockToCache, StorageType storageType) {
      this.blockToCache = blockToCache;
      this.storageType = storageType;
    }
    @Override
    public void run() {
      
      try {
        ReplicaInfo oldReplica = dataset.moveBlockAcrossStorage(blockToCache, storageType);
        if( oldReplica.getState() != ReplicaState.PROVIDED) {
          LOG.error("Block " + blockToCache + " was not provided to being with ");
        }
        
        synchronized (ProvidedCacheManager.this) {
          Value existingValue = blocksForCaching.get(blockToCache);
          
          if (existingValue == null)
            throw new NullPointerException();
          
          if (existingValue.state != State.CACHING && existingValue.state != State.CACHING_CANCELLED)
            throw new IllegalStateException();
          
          if (existingValue.state == State.CACHING_CANCELLED) {
            //TODO have to move the block back!
            LOG.warn("Caching of " + blockToCache + " was cancelled.");
            return;
          }
          
          blocksForCaching.put(blockToCache, new Value(State.CACHED, storageType));
        }
        
        LOG.info("Finished moving block " + blockToCache + " to storage " + storageType);
      } catch (IOException e) {
        LOG.info("Error in caching block " + blockToCache + ": "+ e);
      }
    }
  }
  
  
  public ProvidedCacheManager(FsDatasetImpl dataset) {
    this.dataset = dataset;
    
    //for now, use the same number of threads as FS dataset cache.
    final int maxNumThreads = dataset.datanode.getConf().getInt(
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_KEY,
        DFSConfigKeys.DFS_DATANODE_FSDATASETCACHE_MAX_THREADS_PER_VOLUME_DEFAULT);
    
    ThreadFactory workerFactory = new ThreadFactoryBuilder()
    .setDaemon(true)
    .setNameFormat("ProvidedCacheManager")
    .build();
    
    cacheExecutor = new ThreadPoolExecutor(
      1, maxNumThreads,
      60, TimeUnit.SECONDS,
      new LinkedBlockingQueue<Runnable>(),
      workerFactory);

  }
  
  synchronized void cacheBlock(ExtendedBlock b) {
    
    //TODO have a caching policy here!
    StorageType storageType = StorageType.DISK; //for now, all are cached in disk
    
    Value existingValue = blocksForCaching.get(b);
    if ( existingValue != null) {
      
      if (existingValue.state == State.CACHING || existingValue.state == State.CACHED)
        return;
      
      if (existingValue.state == State.UNCACHING) {
        //TODO
      }
    }
    blocksForCaching.put(b, new Value(State.CACHING, storageType));
    cacheExecutor.execute(new CachingTask(b, storageType));
    LOG.info("Starting to cache block " + b + " to storage of type " + storageType);
  }
  
  synchronized void unCacheBlock (ExtendedBlock b) {
    //TODO
  }
}
