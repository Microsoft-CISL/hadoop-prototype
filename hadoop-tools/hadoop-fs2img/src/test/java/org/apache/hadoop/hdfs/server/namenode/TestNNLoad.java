package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap.BlockProvider;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat.ReaderOptions;
import org.apache.hadoop.hdfs.server.namenode.TreeWalk.TreeIterator;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class TestNNLoad {

  @Rule public TestName name = new TestName();
  public static final Logger LOG = LoggerFactory.getLogger(TestNNLoad.class);

  final Random r = new Random();
  final File fBASE = new File(MiniDFSCluster.getBaseDirectory());
  final Path BASE = new Path(fBASE.toURI().toString());
  final Path BLOCKFILE = new Path(BASE, "blocks.csv");
  final Path NAMEPATH = new Path("file:///home/virajith/Desktop/protobuf/benchmarks");
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

  static Path removePrefix(Path base, Path walk) {
    Path wpath = new Path(walk.toUri().getPath());
    Path bpath = new Path(base.toUri().getPath());
    Path ret = new Path("/");
    while (!(bpath.equals(wpath) || "".equals(wpath.getName()))) {
      ret = "".equals(ret.getName())
        ? new Path("/", wpath.getName())
        : new Path(new Path("/", wpath.getName()),
                   new Path(ret.toString().substring(1)));
      wpath = wpath.getParent();
    }
    if (!bpath.equals(wpath)) {
      throw new IllegalArgumentException(base + " not a prefix of " + walk);
    }
    return ret;
  }

  @Test //(timeout=30000)
  public void testBlockRead() throws Exception {
    createImage(new FSTreeWalk(NAMEPATH, conf), NAMEPATH);
    startCluster(NAMEPATH, 1);
    FileSystem fs = cluster.getFileSystem();
    Thread.sleep(2000);
    int count = 0;
    // read NN metadata, verify contents match
    // TODO NN could write, should find something else to validate
    for (TreePath e : new FSTreeWalk(NAMEPATH, conf)) {
      FileStatus rs = e.getFileStatus();
      Path hp = removePrefix(NAMEPATH, rs.getPath());
      LOG.info("hp " + hp.toUri().getPath());
      //skip HDFS specific files, which may have been created later on.
      if(hp.toString().contains("in_use.lock") || hp.toString().contains("current"))
        continue;
      e.accept(count++);
      assertTrue(fs.exists(hp));
      FileStatus hs = fs.getFileStatus(hp);
      assertEquals(hp.toUri().getPath(), hs.getPath().toUri().getPath());
      assertEquals(rs.getPermission(), hs.getPermission());
      assertEquals(SINGLEUSER, hs.getOwner());
      assertEquals(SINGLEGROUP, hs.getGroup());
      if (rs.isFile()) {
        assertEquals(rs.getLen(), hs.getLen());
        try (ReadableByteChannel i = Channels.newChannel(
              new FileInputStream(new File(rs.getPath().toUri())))) {
          try (ReadableByteChannel j = Channels.newChannel(
                fs.open(hs.getPath()))) {
            ByteBuffer ib = ByteBuffer.allocate(4096);
            ByteBuffer jb = ByteBuffer.allocate(4096);
            while (true) {
              int il = i.read(ib);
              int jl = j.read(jb);
              if (il < 0 || jl < 0) {
                assertEquals(il, jl);
                break;
              }
              ib.flip();
              jb.flip();
              int cmp = Math.min(ib.remaining(), jb.remaining());
              for (int k = 0; k < cmp; ++k) {
                assertEquals(ib.get(), jb.get());
              }
              ib.compact();
              jb.compact();
            }

          }
        }
      }
    }
  }

}
