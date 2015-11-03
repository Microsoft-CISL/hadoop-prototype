package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

public class TestSingleUGIResolver {

  @Rule public TestName name = new TestName();

  static final int testuid = 10101;
  static final int testgid = 10102;
  static final String testuser = "tenaqvyybdhragqvatbf";
  static final String testgroup = "tnyybcvatlnxf";

  SingleUGIResolver ugi = new SingleUGIResolver();

  @Before
  public void setup() {
    Configuration conf = new Configuration(false);
    conf.setInt(SingleUGIResolver.UID, testuid);
    conf.setInt(SingleUGIResolver.GID, testgid);
    conf.set(SingleUGIResolver.USER, testuser);
    conf.set(SingleUGIResolver.GROUP, testgroup);
    ugi.setConf(conf);
    System.out.println(name.getMethodName());
  }

  @Test
  public void testRewrite() {
    FsPermission p1 = new FsPermission((short)0755);
    match(ugi.resolve(file("dingo", "dingo", p1)), p1);
    match(ugi.resolve(file(testuser, "dingo", p1)), p1);
    match(ugi.resolve(file("dingo", testgroup, p1)), p1);
    match(ugi.resolve(file(testuser, testgroup, p1)), p1);

    FsPermission p2 = new FsPermission((short)0x8000);
    match(ugi.resolve(file("dingo", "dingo", p2)), p2);
    match(ugi.resolve(file(testuser, "dingo", p2)), p2);
    match(ugi.resolve(file("dingo", testgroup, p2)), p2);
    match(ugi.resolve(file(testuser, testgroup, p2)), p2);

    Map<Integer,String> ids = ugi.ugiMap();
    assertEquals(2, ids.size());
    assertEquals(testuser, ids.get(10101));
    assertEquals(testgroup, ids.get(10102));
  }

  @Test
  public void testDefault() {
    String user;
    try {
      user = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      user = "hadoop";
    }
    Configuration conf = new Configuration(false);
    ugi.setConf(conf);
    Map<Integer,String> ids = ugi.ugiMap();
    assertEquals(2, ids.size());
    assertEquals(user, ids.get(0));
    assertEquals(user, ids.get(1));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidUid() {
    Configuration conf = ugi.getConf();
    conf.setInt(SingleUGIResolver.UID, (1 << 24) + 1);
    ugi.setConf(conf);
    ugi.resolve(file(testuser, testgroup, new FsPermission((short)0777)));
  }

  @Test(expected=IllegalArgumentException.class)
  public void testInvalidGid() {
    Configuration conf = ugi.getConf();
    conf.setInt(SingleUGIResolver.GID, (1 << 24) + 1);
    ugi.setConf(conf);
    ugi.resolve(file(testuser, testgroup, new FsPermission((short)0777)));
  }

  @Test(expected=IllegalStateException.class)
  public void testDuplicateIds() {
    Configuration conf = new Configuration(false);
    conf.setInt(SingleUGIResolver.UID, 4344);
    conf.setInt(SingleUGIResolver.GID, 4344);
    conf.set(SingleUGIResolver.USER, testuser);
    conf.set(SingleUGIResolver.GROUP, testgroup);
    ugi.setConf(conf);
    ugi.ugiMap();
  }

  static void match(long encoded, FsPermission p) {
    assertEquals(p, new FsPermission((short)(encoded & 0xFFFF)));
    long uid = (encoded >>> UGIResolver.USER_STRID_OFFSET);
    uid &= UGIResolver.USER_GROUP_STRID_MASK;
    assertEquals(testuid, uid);
    long gid = (encoded >>> UGIResolver.GROUP_STRID_OFFSET);
    gid &= UGIResolver.USER_GROUP_STRID_MASK;
    assertEquals(testgid, gid);
  }

  static FileStatus file(String user, String group, FsPermission perm) {
    Path p = new Path("foo://bar:4344/baz/dingo");
    return new FileStatus(
          4344 * (1 << 20),        /* long length,             */
          false,                   /* boolean isdir,           */
          1,                       /* int block_replication,   */
          256 * (1 << 20),         /* long blocksize,          */
          0L,                      /* long modification_time,  */
          0L,                      /* long access_time,        */
          perm,                    /* FsPermission permission, */
          user,                    /* String owner,            */
          group,                   /* String group,            */
          p);                      /* Path path                */
  }

}
