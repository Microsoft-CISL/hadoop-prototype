package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.Path;

public class FileRegion implements BlockAlias {

  final Path path;
  final long offset;
  final long length;
  final String blockId;

  public FileRegion(String blockId, Path path, long offset, long length) {
    this.path = path;
    this.offset = offset;
    this.length = length;
    this.blockId = blockId;
  }

  @Override
  public String getBlockId() {
    return blockId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FileRegion)) {
      return false;
    }
    FileRegion o = (FileRegion) other;
    return blockId.equals(o.blockId)
      && offset == o.offset
      && length == o.length
      && path.equals(o.path);
  }

  @Override
  public int hashCode() {
    return blockId.hashCode();
  }

}
