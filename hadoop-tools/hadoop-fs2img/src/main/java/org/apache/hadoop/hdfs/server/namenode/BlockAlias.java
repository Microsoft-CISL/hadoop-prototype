package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.hdfs.protocol.Block;

public interface BlockAlias {

  Block getBlock();

}
