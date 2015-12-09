package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfoContiguous;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap.BlockProvider;
import org.apache.hadoop.util.ReflectionUtils;

public class BlockFormatProvider extends BlockProvider implements Configurable {

  public static final String BLOCK_CLASS = ImageWriter.Options.BLOCK_CLASS;

  private Configuration conf;
  private BlockFormat<? extends BlockAlias> fmt;

  @Override
  public void setConf(Configuration conf) {
    Class<? extends BlockFormat> c =
      conf.getClass(BLOCK_CLASS, TextFileRegionFormat.class, BlockFormat.class);
    fmt = ReflectionUtils.newInstance(c, conf);
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public Iterator<BlockInfo> iterator() {
    try {
      final BlockFormat.Reader<? extends BlockAlias> r = fmt.getReader(null);
      return new Iterator<BlockInfo>() {

        final Iterator<? extends BlockAlias> inner = r.iterator();

        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }

        @Override
        public BlockInfo next() {
          Block blk = inner.next().getBlock();
          return new BlockInfoContiguous(blk, (short)1);
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
