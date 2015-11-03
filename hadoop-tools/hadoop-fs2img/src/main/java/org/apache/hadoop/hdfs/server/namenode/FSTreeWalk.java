package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FSTreeWalk extends TreeWalk {

  final Path root;
  final FileSystem fs;

  public FSTreeWalk(Path root, Configuration conf) throws IOException {
    this.root = root;
    fs = root.getFileSystem(conf);
  }

  @Override
  protected Iterable<TreePath> getChildren(TreePath p, long id, TreeIterator i) {
    // TODO symlinks
    if (!p.getFileStatus().isDirectory()) {
      return Collections.emptyList();
    }
    try {
      ArrayList<TreePath> ret = new ArrayList<>();
      for (FileStatus s : fs.listStatus(p.getFileStatus().getPath())) {
        ret.add(new TreePath(s, id, i));
      }
      return ret;
    } catch (FileNotFoundException e) {
      throw new ConcurrentModificationException("FS modified");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  class FSTreeIterator extends TreeIterator {

    private FSTreeIterator() {
    }

    FSTreeIterator(TreePath p) {
      pending.addFirst(new TreePath(p.getFileStatus(), p.getParentId(), this));
    }

    FSTreeIterator(Path p) throws IOException {
      try {
        FileStatus s = fs.getFileStatus(root);
        pending.addFirst(new TreePath(s, -1L, this));
      } catch (FileNotFoundException e) {
        if (p.equals(root)) {
          throw e;
        }
        throw new ConcurrentModificationException("FS modified");
      }
    }

    @Override
    public TreeIterator fork() {
      if (pending.isEmpty()) {
        return new FSTreeIterator();
      }
      return new FSTreeIterator(pending.removeFirst());
    }

  }

  @Override
  public TreeIterator iterator() {
    try {
      return new FSTreeIterator(root);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
