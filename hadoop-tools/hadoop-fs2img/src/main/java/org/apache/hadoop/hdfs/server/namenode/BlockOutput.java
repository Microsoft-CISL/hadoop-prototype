package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public abstract class BlockOutput {

  public abstract void store(String ident, Path p, long offset, long len)
    throws IOException;

}
