package org.apache.hadoop.hdfs.server.namenode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;

public abstract class UGIResolver {

  static final int USER_STRID_OFFSET = 40;
  static final int GROUP_STRID_OFFSET = 16;
  static final long USER_GROUP_STRID_MASK = (1 << 24) - 1;
  /**
   * Permission is serialized as a 64-bit long. [0:24):[25:48):[48:64) (in Big
   * Endian).
   * The first and the second parts are the string ids of the user and
   * group name, and the last 16 bits are the permission bits.
   */
  protected final long buildPermissionStatus(
      String owner, String group, short permission) {

    long userId = users.get(owner);
    if (0L != ((~USER_GROUP_STRID_MASK) & userId)) {
      throw new IllegalArgumentException("UID must fit in 24 bits");
    }

    long groupId = groups.get(group);
    if (0L != ((~USER_GROUP_STRID_MASK) & groupId)) {
      throw new IllegalArgumentException("GID must fit in 24 bits");
    }
    return ((userId & USER_GROUP_STRID_MASK) << USER_STRID_OFFSET)
        | ((groupId & USER_GROUP_STRID_MASK) << GROUP_STRID_OFFSET)
        | permission;
  }

  protected final Map<String,Integer> users;
  protected final Map<String,Integer> groups;

  public UGIResolver() {
    this(new HashMap<String,Integer>(), new HashMap<String,Integer>());
  }

  UGIResolver(Map<String,Integer> users, Map<String,Integer> groups) {
    this.users = users;
    this.groups = groups;
  }

  public Map<Integer,String> ugiMap() {
    Map<Integer,String> ret = new HashMap<>();
    for (Map<String,Integer> m : Arrays.asList(users, groups)) {
      for (Map.Entry<String,Integer> e : m.entrySet()) {
        String s = ret.put(e.getValue(), e.getKey());
        if (s != null) {
          throw new IllegalStateException("Duplicate mapping: " +
              e.getValue() + " " + s + " " + e.getKey());
        }
      }
    }
    return ret;
  }

  protected void addUser(String name, int id) {
    Integer uid = users.put(name, id);
    if (uid != null) {
      throw new IllegalArgumentException("Duplicate mapping: " + name +
          " " + uid + " " + id);
    }
  }

  abstract void addUser(String name);
  
  abstract void addGroup(String name);
  
  protected void addGroup(String name, int id) {
    Integer gid = groups.put(name, id);
    if (gid != null) {
      throw new IllegalArgumentException("Duplicate mapping: " + name +
          " " + gid + " " + id);
    }
  }

  public long resolve(FileStatus s) {
    return buildPermissionStatus(user(s), group(s), permission(s).toShort());
  }

  public String user(FileStatus s) {
    return s.getOwner();
  }

  public String group(FileStatus s) {
    return s.getGroup();
  }

  public FsPermission permission(FileStatus s) {
    return s.getPermission();
  }

}
