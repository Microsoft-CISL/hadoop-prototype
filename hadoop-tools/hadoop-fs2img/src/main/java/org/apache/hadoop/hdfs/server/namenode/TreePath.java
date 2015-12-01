package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import com.google.protobuf.ByteString;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INode;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeDirectory;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto.INodeSection.INodeFile;
import static org.apache.hadoop.hdfs.DFSUtil.string2Bytes;
import static org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature.DEFAULT_NAMESPACE_QUOTA;
import static org.apache.hadoop.hdfs.server.namenode.DirectoryWithQuotaFeature.DEFAULT_STORAGE_SPACE_QUOTA;

public class TreePath {
  private long id = -1;
  private final long parentId;
  private final FileStatus stat;
  private final TreeWalk.TreeIterator i;

  protected TreePath(FileStatus stat, long parentId, TreeWalk.TreeIterator i) {
    this.i = i;
    this.stat = stat;
    this.parentId = parentId;
  }

  public FileStatus getFileStatus() {
    return stat;
  }

  public long getParentId() {
    return parentId;
  }

  public long getId() {
    if (id < 0) {
      throw new IllegalStateException();
    }
    return id;
  }

  void accept(long id) {
    this.id = id;
    i.onAccept(this, id);
  }

  public INode toINode(UGIResolver ugi, BlockResolver blk,
      BlockFormat.Writer<FileRegion> out) throws IOException {
    if (stat.isFile()) {
      return toFile(ugi, blk, out);
    } else if (stat.isDirectory()) {
      return toDirectory(ugi);
    } else if (stat.isSymlink()) {
      throw new UnsupportedOperationException("symlinks not supported");
    } else {
      throw new UnsupportedOperationException("Unknown type: " + stat);
    }
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TreePath)) {
      return false;
    }
    TreePath o = (TreePath) other;
    return getParentId() == o.getParentId()
      && getFileStatus().equals(o.getFileStatus());
  }

  @Override
  public int hashCode() {
    long pId = getParentId() * getFileStatus().hashCode();
    return (int)(pId ^ (pId >>> 32));
  }

  void writeBlock(long blockId, long offset, long length,
      BlockFormat.Writer<FileRegion> out) throws IOException {
    String id = "blk_" + blockId;
    FileStatus s = getFileStatus();
    out.store(new FileRegion(id, s.getPath(), offset, length));
  }

  INode toFile(UGIResolver ugi, BlockResolver blk,
      BlockFormat.Writer<FileRegion> out) throws IOException {
    final FileStatus s = getFileStatus();
    INodeFile.Builder b = INodeFile.newBuilder()
        .setReplication(1) //stat.getReplication());
        .setModificationTime(s.getModificationTime())
        .setAccessTime(s.getAccessTime())
        .setPreferredBlockSize(blk.preferredBlockSize(s))
        .setPermission(ugi.resolve(s));
    long off = 0L;
    for (BlockProto block : blk.resolve(s)) {
      b.addBlocks(block);
      writeBlock(block.getBlockId(), off, block.getNumBytes(), out);
      off += block.getNumBytes();
    }
    //optional FileUnderConstructionFeature fileUC = 7;
    //optional AclFeatureProto acl = 8;
    //optional XAttrFeatureProto xAttrs = 9;
    //.setStoragePolicyID(PROVIDED); //stat.getLocalStoragePolicyID());
    INode.Builder ib = INode.newBuilder()
      .setType(INode.Type.FILE)
      .setId(id)
      .setName(ByteString.copyFrom(string2Bytes(s.getPath().getName())))
      .setFile(b);
    return ib.build();
  }

  INode toDirectory(UGIResolver ugi) {
    final FileStatus s = getFileStatus();
    INodeDirectory.Builder b = INodeDirectory.newBuilder()
      .setModificationTime(s.getModificationTime())
      .setNsQuota(DEFAULT_NAMESPACE_QUOTA)
      .setDsQuota(DEFAULT_STORAGE_SPACE_QUOTA)
      .setPermission(ugi.resolve(s));
    INode.Builder ib = INode.newBuilder()
      .setType(INode.Type.DIRECTORY)
      .setId(id)
      .setName(ByteString.copyFrom(string2Bytes(s.getPath().getName())))
      .setDirectory(b);
    return ib.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{ stat=\"").append(getFileStatus()).append("\"");
    sb.append(", id=").append(getId());
    sb.append(", parentId=").append(getParentId());
    sb.append(", iterObjId=").append(System.identityHashCode(i));
    sb.append(" }");
    return sb.toString();
  }
}
