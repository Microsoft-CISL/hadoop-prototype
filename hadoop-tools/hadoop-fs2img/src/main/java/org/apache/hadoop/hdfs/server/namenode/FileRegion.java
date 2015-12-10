package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;
import static org.apache.hadoop.hdfs.protocol.HdfsConstants.GRANDFATHER_GENERATION_STAMP;

public class FileRegion implements BlockAlias {

  final Path path;
  final long offset;
  final long length;
  final long blockId;

  public FileRegion(long blockId, Path path, long offset, long length) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.blockId = blockId;
  }

  @Override
  public Block getBlock() {
    // TODO: use appropriate generation stamp
    return new Block(blockId, length, GRANDFATHER_GENERATION_STAMP);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FileRegion)) {
      return false;
    }
    FileRegion o = (FileRegion) other;
    return blockId == o.blockId
      && offset == o.offset
      && length == o.length
      && path.equals(o.path);
  }

  @Override
  public int hashCode() {
    return (int)(blockId & Integer.MIN_VALUE);
  }

}
