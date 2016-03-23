package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.security.UserGroupInformation;

public class SingleUGIResolver extends UGIResolver implements Configurable {

  public static final String UID   = "hdfs.image.writer.ugi.single.uid";
  public static final String USER  = "hdfs.image.writer.ugi.single.user";
  public static final String GID   = "hdfs.image.writer.ugi.single.gid";
  public static final String GROUP = "hdfs.image.writer.ugi.single.group";

  int uid;
  int gid;
  String user;
  String group;
  Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    uid = conf.getInt(UID, 0);
    user = conf.get(USER);
    if (null == user) {
      try {
        user = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        user = "hadoop";
      }
    }
    gid = conf.getInt(GID, 1);
    group = conf.get(GROUP);
    if (null == group) {
      group = user;
    }
    users.clear();
    groups.clear();
    users.put(user, uid);
    groups.put(group, gid);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String user(FileStatus s) {
    return user;
  }

  @Override
  public String group(FileStatus s) {
    return group;
  }

  @Override
  void addUser(String name) {
    //do nothing 
  }

  @Override
  void addGroup(String name) {
    //do nothing
  }
}
