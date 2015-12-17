package org.apache.hadoop.hdfs.server.common;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;

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
    return new Block(blockId, length, 1001);
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

  public Path getPath() {
    return path;
  }
  
  public long getOffset() {
    return offset;
  }
}
