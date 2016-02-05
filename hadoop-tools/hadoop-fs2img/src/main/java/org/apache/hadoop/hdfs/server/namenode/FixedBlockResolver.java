package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public class FixedBlockResolver extends BlockResolver implements Configurable {

  public static final String BLOCKSIZE   = "hdfs.image.writer.fixed.blocksize";
  public static final String START_BLOCK = "hdfs.image.writer.start.block";

  Configuration conf;
  long blocksize = 256 * (1L << 20);
  final AtomicLong blockIds = new AtomicLong(0);

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    blocksize = conf.getLong(BLOCKSIZE, 256 * (1L << 20));
    blockIds.set(conf.getLong(START_BLOCK, (1L << 30)));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  protected List<Long> blockLengths(FileStatus s) {
    ArrayList<Long> ret = new ArrayList<>();
    if (0 == s.getLen()) {
      return ret;
    }
    int nblocks = (int)((s.getLen() - 1) / blocksize) + 1;
    for (int i = 0; i < nblocks - 1; ++i) {
      ret.add(blocksize);
    }
    long rem = s.getLen() % blocksize;
    ret.add(0 == (rem % blocksize) ? blocksize : rem);
    return ret;
  }

  @Override
  public long nextId() {
    return blockIds.incrementAndGet();
  }

  @Override
  public long lastId() {
    return blockIds.get();
  }

  @Override
  public long preferredBlockSize(FileStatus s) {
    return blocksize;
  }

  @Override
  public int getReplication(FileStatus s) {
	  return 1;
  }
}
