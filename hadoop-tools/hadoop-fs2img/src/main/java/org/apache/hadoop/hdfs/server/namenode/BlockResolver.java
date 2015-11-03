package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;

public abstract class BlockResolver {

  protected BlockProto buildBlock(long blockId, long bytes) {
    return buildBlock(blockId, bytes, 1001);
  }

  protected BlockProto buildBlock(long blockId, long bytes, long genstamp) {
    BlockProto.Builder b = BlockProto.newBuilder()
      .setBlockId(blockId)
      .setNumBytes(bytes)
      .setGenStamp(genstamp);
    return b.build();
  }

  public Iterable<BlockProto> resolve(FileStatus s) {
    List<Long> lengths = blockLengths(s);
    ArrayList<BlockProto> ret = new ArrayList<>(lengths.size());
    long tot = 0;
    for (long l : lengths) {
      tot += l;
      ret.add(buildBlock(nextId(), l));
    }
    if (tot != s.getLen()) {
      // log a warning?
      throw new IllegalStateException(
          "Expected " + s.getLen() + " found " + tot);
    }
    return ret;
  }

  public abstract long nextId();

  /**
   * Gets the maximum sequentially allocated block ID for this filesystem.
   */
  protected abstract long lastId();

  protected abstract List<Long> blockLengths(FileStatus s);


  public long preferredBlockSize(FileStatus s) {
    return s.getBlockSize();
  }

}
