package org.apache.hadoop.hdfs.server.namenode;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

public class TestFixedBlockResolver {

  @Rule public TestName name = new TestName();

  FixedBlockResolver blockId = new FixedBlockResolver();

  @Before
  public void setup() {
    Configuration conf = new Configuration(false);
    conf.setLong(FixedBlockResolver.BLOCKSIZE, 512L * (1L << 20));
    conf.setLong(FixedBlockResolver.START_BLOCK, 512L * (1L << 20));
    blockId.setConf(conf);
    System.out.println(name.getMethodName());
  }

  @Test
  public void testExactBlock() throws Exception {
    FileStatus f = file(512, 256);
    int nblocks = 0;
    for (BlockProto b : blockId.resolve(f)) {
      ++nblocks;
      assertEquals(512L * (1L << 20), b.getNumBytes());
    }
    assertEquals(1, nblocks);

    FileStatus g = file(1024, 256);
    nblocks = 0;
    for (BlockProto b : blockId.resolve(g)) {
      ++nblocks;
      assertEquals(512L * (1L << 20), b.getNumBytes());
    }
    assertEquals(2, nblocks);

    FileStatus h = file(5120, 256);
    nblocks = 0;
    for (BlockProto b : blockId.resolve(h)) {
      ++nblocks;
      assertEquals(512L * (1L << 20), b.getNumBytes());
    }
    assertEquals(10, nblocks);
  }

  @Test
  public void testEmpty() throws Exception {
    FileStatus f = file(0, 100);
    for (BlockProto b : blockId.resolve(f)) {
      fail("Unexpected block for empty file " + b);
    }
  }

  @Test
  public void testRandomFile() throws Exception {
    Random r = new Random();
    long seed = r.nextLong();
    System.out.println("seed: " + seed);
    r.setSeed(seed);

    int len = r.nextInt(4096) + 512;
    int blk = r.nextInt(len - 128) + 128;
    FileStatus s = file(len, blk);
    long nbytes = 0;
    for (BlockProto b : blockId.resolve(s)) {
      nbytes += b.getNumBytes();
      assertTrue(512L * (1L << 20) >= b.getNumBytes());
    }
    assertEquals(s.getLen(), nbytes);
  }

  FileStatus file(long lenMB, long blocksizeMB) {
    Path p = new Path("foo://bar:4344/baz/dingo");
    return new FileStatus(
          lenMB * (1 << 20),       /* long length,             */
          false,                   /* boolean isdir,           */
          1,                       /* int block_replication,   */
          blocksizeMB * (1 << 20), /* long blocksize,          */
          0L,                      /* long modification_time,  */
          0L,                      /* long access_time,        */
          null,                    /* FsPermission permission, */
          "hadoop",                /* String owner,            */
          "hadoop",                /* String group,            */
          p);                      /* Path path                */
  }

}
