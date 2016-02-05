package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;

public class FixedBlockMultiReplicaResolver extends FixedBlockResolver {

  public static final String REPLICATION   = "hdfs.image.writer.fixed.replication";
  
  int replication;
  @Override
  public void setConf(Configuration conf) {
	super.setConf(conf);
	replication = conf.getInt(REPLICATION, 1); 
  }

  public int getReplication(FileStatus s) {
	  return replication;
  }
}
