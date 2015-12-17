package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat;
import org.apache.hadoop.hdfs.server.datanode.ProvidedReplica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInfo;
import org.apache.hadoop.util.ReflectionUtils;

public class ProvidedBlockManager {

  static final Log LOG = LogFactory.getLog(ProvidedBlockManager.class);
  
  ReplicaMap replicaMap;
  FsVolumeImpl providedVolume;

  FileRegionProvider provider;
  Configuration conf;
  
  ProvidedBlockManager(ReplicaMap replicaMap, FsVolumeImpl providedVolume, Configuration conf) {
    //may not be safe to add over replicaMap here; TODO alt. impl? 
    this.replicaMap = replicaMap;
    this.providedVolume = providedVolume;
   
    Class<? extends FileRegionProvider> fmt =
        conf.getClass(DFSConfigKeys.DFS_NAMENODE_BLK_PROVIDER_CLASS, TextFileRegionProvider.class, TextFileRegionProvider.class);
    
    provider = ReflectionUtils.newInstance(fmt, conf);
    this.conf = conf;
  }

  /**
   * Assumed that this is called with a lock on FsDatasetImpl object
   * Modifies replica map of FsDatasetImpl
   * @param blkPoolId the block pool id for which we need to populate blocks
   */
  void readBlocks(String blkPoolId) {
    //TODO read blocks for the specific blk pool id 

    Iterator<FileRegion> iter = provider.iterator();
    //TODO check if the offset is always 0??
    while(iter.hasNext()) {
      FileRegion region = iter.next();
      ReplicaInfo info = new ProvidedReplica(region.getBlock().getBlockId(), region.getPath().toUri(), 
          region.getOffset(), region.getBlock().getNumBytes(), 1001, providedVolume, conf);
      LOG.info("Adding block info " + info);
      replicaMap.add(blkPoolId, info);
    }
  }
  
  public static class FileRegionProvider implements Iterable<FileRegion> {

    @Override
    public Iterator<FileRegion> iterator() {
      // TODO Auto-generated method stub
      return Collections.emptyListIterator();
    }
    
  }
  
  public static class TextFileRegionProvider extends FileRegionProvider implements Configurable {

    private Configuration conf;
    private TextFileRegionFormat fmt;

    @Override
    public void setConf(Configuration conf) {
      fmt = new TextFileRegionFormat();
      fmt.setConf(conf);
      this.conf = conf;
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public Iterator<FileRegion> iterator() {
      try {
        final BlockFormat.Reader<FileRegion> r = fmt.getReader(null);
        return new Iterator<FileRegion>() {

          final Iterator<FileRegion> inner = r.iterator();

          @Override
          public boolean hasNext() {
            return inner.hasNext();
          }

          @Override
          public FileRegion next() {
            return inner.next();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      } catch (IOException e) {
        throw new RuntimeException("Failed to read provided blocks", e);
      }
    }

  }

}
