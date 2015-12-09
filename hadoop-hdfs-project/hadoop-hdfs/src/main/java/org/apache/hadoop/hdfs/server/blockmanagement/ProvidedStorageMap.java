package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;

public class ProvidedStorageMap {

  public static final String FMT = "hdfs.namenode.block.provider";

  public abstract static class BlockProvider implements Iterable<BlockInfo> {
  }

  private final DatanodeStorageInfo storage;

  ProvidedStorageMap(Configuration conf) throws IOException {
    // load block reader into storage
    Class<? extends BlockProvider> fmt =
      conf.getClass(FMT, null, BlockProvider.class);
    if (null == fmt) {
      // disable mapping
      storage = null;
      return;
    }
    storage = new DatanodeStorageInfo(null, null);
  }

  DatanodeStorageInfo getStorage(DatanodeDescriptor dn, DatanodeStorage s) {
    return dn.getStorageInfo(s.getStorageID());
  }

}
