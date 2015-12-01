package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.ImageWriter;
import org.apache.hadoop.hdfs.server.namenode.FixedBlockResolver;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

public class TestNNLoad {

  @Rule public TestName name = new TestName();

  Random r = new Random();

  @Before
  public void setSeed() {
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
  }

  @Test // (timeout = 30000)
  public void testLoadImage() throws Exception {
    
    final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
    final Path BASE = new Path(fBASE.toURI().toString());
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }
    final Path NAMEPATH = new Path(BASE, "hdfs/name");
    final Path BLOCKFILE = new Path(BASE, "blocks.csv");

    Configuration conf = new HdfsConfiguration();
    //conf.setBoolean(NNTOP_ENABLED_KEY, false);
    conf.set(TextFileRegionFormat.WriterOptions.FILEPATH, BLOCKFILE.toString());

    ImageWriter.Options opts = ImageWriter.defaults();
    opts.setConf(conf);
    opts.output(NAMEPATH.toString())
        .blocks(TextFileRegionFormat.class)
        .ugi(SingleUGIResolver.class)
        .blockIds(FixedBlockResolver.class);

    final long seed = r.nextLong();
    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : new RandomTreeWalk(seed)) {
        w.accept(e);
      }
    }

    //conf.set(DFS_NAMENODE_NAME_DIR_KEY, NAMEPATH.toString());
    //MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
    //  .format(false)
    //  .manageNameDfsDirs(false)
    //  .build();
    //cluster.waitNameNodeUp(0);
  }

}
