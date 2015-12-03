package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

  final Random r = new Random();
  final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
  final Path BASE = new Path(fBASE.toURI().toString());

  @Before
  public void setSeed() throws Exception {
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
  }

  @Test(timeout = 20000)
  public void testLoadImage() throws Exception {
    final String USER = "dingo";
    final String GROUP = "yak";
    Configuration conf = new HdfsConfiguration();
    conf.set(SingleUGIResolver.USER, USER);
    conf.set(SingleUGIResolver.GROUP, GROUP);

    final Path BLOCKFILE = new Path(BASE, "blocks.csv");
    conf.set(TextFileRegionFormat.WriterOptions.FILEPATH, BLOCKFILE.toString());

    final Path NAMEPATH = new Path(BASE, "hdfs/name");
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

    conf.set(DFS_NAMENODE_NAME_DIR_KEY, NAMEPATH.toString());
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster.Builder(conf)
        .format(false)
        .manageNameDfsDirs(false)
        .numDataNodes(0)
        .build();
      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      for (TreePath e : new RandomTreeWalk(seed)) {
        FileStatus rs = e.getFileStatus();
        Path hp = new Path(rs.getPath().toUri().getPath());
        assertTrue(fs.exists(hp));
        FileStatus hs = fs.getFileStatus(hp);
        assertEquals(rs.getPath().toUri().getPath(),
                     hs.getPath().toUri().getPath());
        assertEquals(rs.getPermission(), hs.getPermission());
        // TODO: loaded later? Not reflected, yet.
        //assertEquals(rs.getReplication(), hs.getReplication());
        //assertEquals(rs.getBlockSize(), hs.getBlockSize());
        assertEquals(rs.getLen(), hs.getLen());
        assertEquals(USER, hs.getOwner());
        assertEquals(GROUP, hs.getGroup());
        assertEquals(rs.getAccessTime(), hs.getAccessTime());
        assertEquals(rs.getModificationTime(), hs.getModificationTime());
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown(true, true);
      }
    }
  }

}
