package org.apache.hadoop.hdfs.server.namenode;

import java.util.HashSet;
import java.util.Set;

public class FsUGIResolver extends UGIResolver {

  int id;
  Set<String> usernames;
  Set<String> groupnames;
  
  FsUGIResolver() {
    super();
    usernames = new HashSet<String>();
    groupnames = new HashSet<String>();
    id = 0;
  }
  
  @Override
  void addUser(String name) {
    if (!usernames.contains(name)) {
      addUser(name, id);
      id++;
      usernames.add(name);
    }      
  }

  @Override
  void addGroup(String name) {
    if (!groupnames.contains(name)) {
      addGroup(name, id);
      id++;
      groupnames.add(name);
    } 
  }

}
