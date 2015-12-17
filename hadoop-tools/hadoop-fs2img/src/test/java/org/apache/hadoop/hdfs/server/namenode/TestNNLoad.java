package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap.BlockProvider;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat.ReaderOptions;
import org.apache.hadoop.hdfs.server.namenode.ImageWriter;
import org.apache.hadoop.hdfs.server.namenode.FixedBlockResolver;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import org.junit.After;
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
  final Path BLOCKFILE = new Path(BASE, "blocks.csv");
  final Path NAMEPATH = new Path(BASE, "hdfs/name");
  final String SINGLEUSER = "dingo";
  final String SINGLEGROUP = "yak";

  Configuration conf;
  MiniDFSCluster cluster;
  public static final String PROVIDER = "hdfs.namenode.block.provider.class";
  public static final String STORAGE_ID = "hdfs.namenode.block.provider.id";

  @Before
  public void setSeed() throws Exception {
    if (fBASE.exists() && !FileUtil.fullyDelete(fBASE)) {
      throw new IOException("Could not fully delete " + fBASE);
    }
    long seed = r.nextLong();
    r.setSeed(seed);
    System.out.println(name.getMethodName() + " seed: " + seed);
    conf = new HdfsConfiguration();
    conf.set(SingleUGIResolver.USER, SINGLEUSER);
    conf.set(SingleUGIResolver.GROUP, SINGLEGROUP);
    conf.set(TextFileRegionFormat.WriterOptions.FILEPATH, BLOCKFILE.toString());
    conf.setClass(PROVIDER, BlockFormatProvider.class, BlockProvider.class);
    conf.set(STORAGE_ID, DFSConfigKeys.DFS_NAMENODE_PROVIDED_STORAGEUUID);
    conf.setBoolean(DFSConfigKeys.DFS_DATANODE_PROVIDED, true);
    conf.set(ReaderOptions.FILEPATH, BLOCKFILE.toString());
    conf.set(ReaderOptions.DELIMITER, ",");
  }

  @After
  public void shutdown() throws Exception {
    try {
      if (cluster != null) {
        cluster.shutdown(true, true);
      }
    } finally {
      cluster = null;
    }
  }

  void createImage(TreeWalk t, Path out) throws Exception {
    ImageWriter.Options opts = ImageWriter.defaults();
    opts.setConf(conf);
    opts.output(out.toString())
        .blocks(TextFileRegionFormat.class)
        .ugi(SingleUGIResolver.class)
        .blockIds(FixedBlockResolver.class);
    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : t) {
        w.accept(e);
      }
    }
  }

  void startCluster(Path nspath, int numDatanodes) throws IOException {
    conf.set(DFS_NAMENODE_NAME_DIR_KEY, nspath.toString());
    cluster = new MiniDFSCluster.Builder(conf)
      .format(false)
      .manageNameDfsDirs(false)
      .numDataNodes(numDatanodes)
      .build();
    cluster.waitActive();
  }

  @Test(timeout = 20000)
  public void testLoadImage() throws Exception {
    final long seed = r.nextLong();
    createImage(new RandomTreeWalk(seed), NAMEPATH);
    startCluster(NAMEPATH, 0);

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
      assertEquals(SINGLEUSER, hs.getOwner());
      assertEquals(SINGLEGROUP, hs.getGroup());
      assertEquals(rs.getAccessTime(), hs.getAccessTime());
      assertEquals(rs.getModificationTime(), hs.getModificationTime());
    }
  }

  @Test(timeout=20000)
  public void testBlockLoad() throws Exception {
    createImage(new FSTreeWalk(NAMEPATH, conf), NAMEPATH);
    startCluster(NAMEPATH, 1);
  }

  @Test
  public void testBlockRead() throws Exception {
    // TODO: make this into a test
    createImage(new FSTreeWalk(NAMEPATH, conf), NAMEPATH);
    startCluster(NAMEPATH, 1);
    FileSystem fs = cluster.getFileSystem();
    List<Path> l = new LinkedList<>();
    l.add(new Path("/"));
    while (!l.isEmpty()) {
      Path p = l.remove(0);
      System.out.println("PATH: " + p);
      for (FileStatus f : fs.listStatus(p)) {
        if (p.equals(f.getPath())) {
          continue;
        }
        if ("VERSION".equals(f.getPath().getName())) {
          for (BlockLocation b : fs.getFileBlockLocations(f, 0, f.getLen())) {
            System.out.println("VERSION BLOCK: " + b);
          }
          try (BufferedReader i = new BufferedReader(
                new InputStreamReader(fs.open(f.getPath())))) {
            String s;
            while (null != (s = i.readLine())) {
              System.out.println(s);
            }
          }
          Thread.sleep(1000);
          for (BlockLocation b : fs.getFileBlockLocations(f, 0, f.getLen())) {
            for (StorageType t : b.getStorageTypes()) {
              System.out.println("VERSION BLOCK: " + b + " type: " + t);
            }
          }

        }
        l.add(0, f.getPath());
      }
    }
  }

}
