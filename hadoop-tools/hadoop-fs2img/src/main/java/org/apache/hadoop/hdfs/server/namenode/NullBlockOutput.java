package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class NullBlockOutput extends BlockOutput {

  @Override
  public void store(String ident, Path p, long offset, long len)
      throws IOException {
    // ignore
    //StringBuilder sb = new StringBuilder();
    //sb.append(ident).append(" ")
    //  .append(p).append(" @ ")
    //  .append(offset).append(" ").append(len);
    //System.out.println(sb.toString());
  }

}
