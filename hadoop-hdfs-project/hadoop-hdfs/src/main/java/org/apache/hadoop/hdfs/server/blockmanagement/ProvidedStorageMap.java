package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfoWithStorage;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage.State;
import org.apache.hadoop.hdfs.util.RwLock;
import org.apache.hadoop.util.ReflectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class ProvidedStorageMap {

  private static final Logger LOG = LoggerFactory.getLogger(ProvidedStorageMap.class);

  public static final String FMT = "hdfs.namenode.block.provider.class";
  public static final String STORAGE_ID = "hdfs.namenode.block.provider.id";

  // limit to a single provider for now
  private final BlockProvider p;
  private final String storageId;
  private final ProvidedDescriptor dns;
  private final DatanodeStorageInfo storage;

  ProvidedStorageMap(RwLock lock, BlockManager bm, Configuration conf)
      throws IOException {
    storageId = conf.get(STORAGE_ID);
    if (null == storageId) {
      // disable mapping
      p = null;
      dns = null;
      storage = null;
      return;
    }
    //State.READ_ONLY_SHARED ?
    DatanodeStorage ds =
      new DatanodeStorage(storageId, State.NORMAL, StorageType.PROVIDED);
    // TODO need a subclass for DNDesc, DNSI, or both
    dns = new ProvidedDescriptor();
    storage = dns.createProvidedStorage(ds);

    // load block reader into storage
    Class<? extends BlockProvider> fmt =
      conf.getClass(FMT, BlockProvider.class, BlockProvider.class);
    p = ReflectionUtils.newInstance(fmt, conf);
    p.init(lock, bm, storage);
    LOG.info("Loaded block provider class: " + p.getClass() + " storage: " + storage);
  }

  // TODO: need to capture DN registration w/ storage
  //       possibly snare from getStorage

  DatanodeStorageInfo getStorage(DatanodeDescriptor dn, DatanodeStorage s)
      throws IOException {
    if (storageId != null && storageId.equals(s.getStorageID())) {
      if (StorageType.PROVIDED.equals(s.getStorageType())) {
        // poll service, initiate 
        p.start();
        dn.injectStorage(storage);
        return dns.getProvidedStorage(dn, s);
      }
      LOG.warn("Reserved storage {} reported as non-provided from {}", s, dn);
    }
    return dn.getStorageInfo(s.getStorageID());
  }

  public LocatedBlockBuilder newLocatedBlocks(int maxValue) {
    if (null == storageId) {
      return new LocatedBlockBuilder(maxValue);
    }
    return new ProvidedBlocksBuilder(maxValue);
  }

  class ProvidedBlocksBuilder extends LocatedBlockBuilder {

    ShadowDNIWS pending;

    ProvidedBlocksBuilder(int maxBlocks) {
      super(maxBlocks);
      pending = new ShadowDNIWS(dns, storageId);
    }

    @Override
    LocatedBlock newLocatedBlock(ExtendedBlock eb,
        DatanodeStorageInfo[] storages, long pos, boolean isCorrupt) {
      DatanodeInfoWithStorage[] locs =
        new DatanodeInfoWithStorage[storages.length];
      String[] sids = new String[storages.length];
      StorageType[] types = new StorageType[storages.length];
      for (int i = 0; i < storages.length; ++i) {
        sids[i] = storages[i].getStorageID();
        types[i] = storages[i].getStorageType();
        if (StorageType.PROVIDED.equals(storages[i].getStorageType())) {
          locs[i] = pending;
        } else {
          locs[i] = new DatanodeInfoWithStorage(
              storages[i].getDatanodeDescriptor(), sids[i], types[i]);
        }
      }
      return new LocatedBlock(eb, locs, sids, types, pos, isCorrupt, null);
    }

    @Override
    LocatedBlocks build(DatanodeDescriptor client) {
      // TODO: to support multiple provided storages, need to pass/maintain map
      // set all fields of pending DatanodeInfo
      
      List<String> excludedUUids = new ArrayList<String>();
      for (LocatedBlock b: blocks) {
        DatanodeInfo[] infos = b.getLocations();
        StorageType[] types = b.getStorageTypes();
        
        for (int i = 0; i < types.length; i++) {
          if (!StorageType.PROVIDED.equals(types[i])) {
            excludedUUids.add(infos[i].getDatanodeUuid());
          }
        }
      }
      
      DatanodeDescriptor dn = dns.choose(client, excludedUUids);
      if (dn == null)
        dn = dns.choose(client);
      
      pending.replaceInternal(dn);
      return new LocatedBlocks(flen, isUC, blocks, last, lastComplete, feInfo);
    }
    @Override
    LocatedBlocks build() {
      return build(dns.chooseRandom());
    }
  }

  static class ShadowDNIWS extends DatanodeInfoWithStorage {
    String shadowUuid;

    ShadowDNIWS(DatanodeDescriptor d, String storageId) {
      super(d, storageId, StorageType.PROVIDED);
    }

    @Override
    public String getDatanodeUuid() {
      return shadowUuid;
    }
    public void setDatanodeUuid(String uuid) {
      shadowUuid = uuid;
    }
    void replaceInternal(DatanodeDescriptor dn) {
      updateRegInfo(dn); // overwrite DatanodeID (except UUID)
      setDatanodeUuid(dn.getDatanodeUuid());
      setCapacity(dn.getCapacity());
      setDfsUsed(dn.getDfsUsed());
      setRemaining(dn.getRemaining());
      setBlockPoolUsed(dn.getBlockPoolUsed());
      setCacheCapacity(dn.getCacheCapacity());
      setCacheUsed(dn.getCacheUsed());
      setLastUpdate(dn.getLastUpdate());
      setLastUpdateMonotonic(dn.getLastUpdateMonotonic());
      setXceiverCount(dn.getXceiverCount());
      setNetworkLocation(dn.getNetworkLocation());
      adminState = dn.getAdminState();
      setUpgradeDomain(dn.getUpgradeDomain());
    }

  }

  // NOTE: never resolved through registerDatanode, so not in the topology
  static class ProvidedDescriptor extends DatanodeDescriptor {

    final NavigableMap<String,DatanodeDescriptor> dns =
      new ConcurrentSkipListMap<>();

    ProvidedDescriptor() {
      super(new DatanodeID(
            null,                         // String ipAddr,
            null,                         // String hostName,
            UUID.randomUUID().toString(), // String datanodeUuid,
            0,                            // int xferPort,
            0,                            // int infoPort,
            0,                            // int infoSecurePort,
            0));                          // int ipcPort
    }

    // TODO: examine BM::getBlocksWithLocations (used by rebalancer?)

    DatanodeStorageInfo getProvidedStorage(
        DatanodeDescriptor dn, DatanodeStorage s) {
      dns.put(dn.getDatanodeUuid(), dn);
      // TODO: maintain separate RPC ident per dn
      return storageMap.get(s.getStorageID());
    }

    DatanodeStorageInfo createProvidedStorage(DatanodeStorage ds) {
      assert null == storageMap.get(ds.getStorageID());
      DatanodeStorageInfo storage = new DatanodeStorageInfo(this, ds);
      storage.setHeartbeatedSinceFailover(true);
      storageMap.put(storage.getStorageID(), storage);
      return storage;
    }

    DatanodeDescriptor choose(DatanodeDescriptor client) {
      // exact match for now
      DatanodeDescriptor dn = dns.get(client.getDatanodeUuid());
      if (null == dn) {
        dn = chooseRandom();
      }
      return dn;
    }
    
    DatanodeDescriptor choose(DatanodeDescriptor client, List<String> excludedUUids) {
      // exact match for now
      DatanodeDescriptor dn = dns.get(client.getDatanodeUuid());
      
      if (null == dn || excludedUUids.contains(client.getDatanodeUuid())) {
        dn = null;
        Set<String> exploredUUids = new HashSet<String>();
        
        while(exploredUUids.size() < dns.size()) {
          Map.Entry<String, DatanodeDescriptor> d =
                  dns.ceilingEntry(UUID.randomUUID().toString());
          if (null == d) {
            d = dns.firstEntry();
          }
          String uuid = d.getValue().getDatanodeUuid();
          //this node has already been explored, and was not selected earlier
          if (exploredUUids.contains(uuid))
            continue;
          exploredUUids.add(uuid);
          //this node has been excluded
          if (excludedUUids.contains(uuid))
            continue;
          return dns.get(uuid);
        }
      }
      
      return dn;
    }


    DatanodeDescriptor chooseRandom(DatanodeStorageInfo[] excludedStorages) {
      // XXX Not uniformally random; skewed toward sparse sections of the ids
      Set<DatanodeDescriptor> excludedNodes = new HashSet<DatanodeDescriptor>();
      if (excludedStorages != null) {
  	    for(int i=0; i < excludedStorages.length; i++) {
  	    	LOG.info("Excluded: " + excludedStorages[i].getDatanodeDescriptor());
  	      excludedNodes.add(excludedStorages[i].getDatanodeDescriptor());
  	    }
      }
      Set<DatanodeDescriptor> exploredNodes = new HashSet<DatanodeDescriptor>();
      
      while(exploredNodes.size() < dns.size()) {
      	Map.Entry<String, DatanodeDescriptor> d =
      		      dns.ceilingEntry(UUID.randomUUID().toString());
  	    if (null == d) {
  	      d = dns.firstEntry();
  	    }
  	    DatanodeDescriptor node = d.getValue();
  	    //this node has already been explored, and was not selected earlier
  	    if (exploredNodes.contains(node))
  	    	continue;
  	    exploredNodes.add(node);
  	    //this node has been excluded
  	    if (excludedNodes.contains(node))
  	    	continue;
  	    return node;
      }
      
      return null;
    }
    
    DatanodeDescriptor chooseRandom() {
    	return chooseRandom(null);
    }
    // TODO: recovery? invalidation?

    @Override
    void addBlockToBeReplicated(Block block, DatanodeStorageInfo[] targets) {
      // pick a random datanode, delegate to it
      DatanodeDescriptor node = chooseRandom(targets);
      if (node != null) {
        node.addBlockToBeReplicated(block, targets);
      }
      else {
      	//TODO throw an exception!!!
      	//TODO if we instrument each DN to hold multiple replicas, will this case ever arise??
        LOG.error("Cannot find a source node to replicate block: " + block + " from");
      }
    }

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
    // XXX need to respect DN lifecycle, proper concurrency
    boolean hasDNs = false;

    void init(RwLock lock, BlockManager bm, DatanodeStorageInfo storage) {
      this.bm = bm;
      this.lock = lock;
      this.storage = storage;
    }

    void start() throws IOException {
      assert lock.hasWriteLock() : "Not holding write lock";
      if (hasDNs) {
        return;
      }
      LOG.info("Calling process first blk report from storage: " + storage);
      // first pass; periodic refresh should call bm.processReport
      bm.processFirstBlockReport(storage, new ProvidedBlockList(iterator()));
      hasDNs = true;

      // create background thread to read from fmt
      // forall blocks
      //   batch ProvidedBlockList from BlockProvider
      //   report BM::processFirstBlockReport
      //   subsequently report BM::processReport
    }

    @Override
    public Iterator<Block> iterator() {
      // default to empty storage
      assert false : "Should be overridden";
      return Collections.emptyListIterator();
    }

  }

}
