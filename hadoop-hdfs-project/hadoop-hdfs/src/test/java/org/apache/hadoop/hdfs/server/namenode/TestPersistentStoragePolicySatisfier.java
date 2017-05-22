/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.junit.Test;

import java.io.IOException;

import static org.apache.hadoop.hdfs.server.common.HdfsServerConstants.XATTR_SATISFY_STORAGE_POLICY;

/**
 * Test persistence of satisfying files/directories.
 */
public class TestPersistentStoragePolicySatisfier {

  private static Configuration conf;

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  private static Path testFile =
      new Path("/testFile");
  private static String testFileName = testFile.toString();

  private static Path parentDir = new Path("/parentDir");
  private static Path parentFile = new Path(parentDir, "parentFile");
  private static String parentFileName = parentFile.toString();
  private static Path childDir = new Path(parentDir, "childDir");
  private static Path childFile = new Path(childDir, "childFile");
  private static String childFileName = childFile.toString();

  private static final String COLD = "COLD";
  private static final String WARM = "WARM";
  private static final String ONE_SSD = "ONE_SSD";
  private static final String ALL_SSD = "ALL_SSD";

  private static StorageType[][] storageTypes = new StorageType[][] {
      {StorageType.ARCHIVE, StorageType.DISK},
      {StorageType.DISK, StorageType.SSD},
      {StorageType.SSD, StorageType.RAM_DISK},
      {StorageType.ARCHIVE, StorageType.DISK},
      {StorageType.ARCHIVE, StorageType.SSD}
  };

  private final int timeout = 300000;

  /**
   * Setup environment for every test case.
   * @throws IOException
   */
  public void clusterSetUp() throws Exception {
    clusterSetUp(false, new HdfsConfiguration());
  }

  /**
   * Setup environment for every test case.
   * @param hdfsConf hdfs conf.
   * @throws Exception
   */
  public void clusterSetUp(Configuration hdfsConf) throws Exception {
    clusterSetUp(false, hdfsConf);
  }

  /**
   * Setup cluster environment.
   * @param isHAEnabled if true, enable simple HA.
   * @throws IOException
   */
  private void clusterSetUp(boolean isHAEnabled, Configuration newConf)
      throws Exception {
    conf = newConf;
    final int dnNumber = storageTypes.length;
    final short replication = 3;
    MiniDFSCluster.Builder clusterBuilder = new MiniDFSCluster.Builder(conf)
        .storageTypes(storageTypes)
        .numDataNodes(dnNumber);
    if (isHAEnabled) {
      clusterBuilder.nnTopology(MiniDFSNNTopology.simpleHATopology());
    }
    cluster = clusterBuilder.build();
    cluster.waitActive();
    if (isHAEnabled) {
      cluster.transitionToActive(0);
      fs = HATestUtil.configureFailoverFs(cluster, conf);
    } else {
      fs = cluster.getFileSystem();
    }

    createTestFiles(fs, replication);
  }

  /**
   * Setup test files for testing.
   * @param dfs
   * @param replication
   * @throws Exception
   */
  private void createTestFiles(DistributedFileSystem dfs,
      short replication) throws Exception {
    DFSTestUtil.createFile(dfs, testFile, 1024L, replication, 0L);
    DFSTestUtil.createFile(dfs, parentFile, 1024L, replication, 0L);
    DFSTestUtil.createFile(dfs, childFile, 1024L, replication, 0L);

    DFSTestUtil.waitReplication(dfs, testFile, replication);
    DFSTestUtil.waitReplication(dfs, parentFile, replication);
    DFSTestUtil.waitReplication(dfs, childFile, replication);
  }

  /**
   * Tear down environment for every test case.
   * @throws IOException
   */
  private void clusterShutdown() throws IOException{
    if(fs != null) {
      fs.close();
      fs = null;
    }
    if(cluster != null) {
      cluster.shutdown(true);
      cluster = null;
    }
  }

  /**
   * While satisfying file/directory, trigger the cluster's checkpoint to
   * make sure satisfier persistence work as expected. This test case runs
   * as below:
   * 1. use satisfyStoragePolicy and add xAttr to the file.
   * 2. do the checkpoint by secondary NameNode.
   * 3. restart the cluster immediately.
   * 4. make sure all the storage policies are satisfied.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testWithCheckpoint() throws Exception {
    try {
      clusterSetUp();
      fs.setStoragePolicy(testFile, WARM);
      fs.satisfyStoragePolicy(testFile);

      // Start the checkpoint.
      conf.set(
          DFSConfigKeys.DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_KEY, "0.0.0.0:0");
      SecondaryNameNode secondary = new SecondaryNameNode(conf);
      secondary.doCheckpoint();
      restartCluster();

      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.DISK, 1, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.ARCHIVE, 2, timeout, fs);

      fs.setStoragePolicy(parentDir, COLD);
      fs.satisfyStoragePolicy(parentDir);

      DFSTestUtil.waitExpectedStorageType(
          parentFileName, StorageType.ARCHIVE, 3, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          childFileName, StorageType.DEFAULT, 3, timeout, fs);

    } finally {
      clusterShutdown();
    }
  }

  /**
   * Tests to verify satisfier persistence working as expected
   * in HA env. This test case runs as below:
   * 1. setup HA cluster env with simple HA topology.
   * 2. switch the active NameNode from nn0/nn1 to nn1/nn0.
   * 3. make sure all the storage policies are satisfied.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testWithHA() throws Exception {
    try {
      // Enable HA env for testing.
      clusterSetUp(true, new HdfsConfiguration());

      fs.setStoragePolicy(testFile, ALL_SSD);
      fs.satisfyStoragePolicy(testFile);

      cluster.transitionToStandby(0);
      cluster.transitionToActive(1);

      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.SSD, 3, timeout, fs);

      // test directory
      fs.setStoragePolicy(parentDir, WARM);
      fs.satisfyStoragePolicy(parentDir);
      cluster.transitionToStandby(1);
      cluster.transitionToActive(0);

      DFSTestUtil.waitExpectedStorageType(
          parentFileName, StorageType.DISK, 1, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          parentFileName, StorageType.ARCHIVE, 2, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          childFileName, StorageType.DEFAULT, 3, timeout, fs);
    } finally {
      clusterShutdown();
    }
  }


  /**
   * Tests to verify satisfier persistence working well with multiple
   * restarts operations. This test case runs as below:
   * 1. satisfy the storage policy of file1.
   * 2. restart the cluster.
   * 3. check whether all the blocks are satisfied.
   * 4. satisfy the storage policy of file2.
   * 5. restart the cluster.
   * 6. check whether all the blocks are satisfied.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testWithRestarts() throws Exception {
    try {
      clusterSetUp();
      fs.setStoragePolicy(testFile, ONE_SSD);
      fs.satisfyStoragePolicy(testFile);
      restartCluster();
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.SSD, 1, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.DISK, 2, timeout, fs);

      // test directory
      fs.setStoragePolicy(parentDir, COLD);
      fs.satisfyStoragePolicy(parentDir);
      restartCluster();
      DFSTestUtil.waitExpectedStorageType(
          parentFileName, StorageType.ARCHIVE, 3, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          childFileName, StorageType.DEFAULT, 3, timeout, fs);
    } finally {
      clusterShutdown();
    }
  }

  /**
   * Tests to verify satisfier persistence working well with
   * federal HA env. This test case runs as below:
   * 1. setup HA test environment with federal topology.
   * 2. satisfy storage policy of file1.
   * 3. switch active NameNode from nn0 to nn1.
   * 4. switch active NameNode from nn2 to nn3.
   * 5. check whether the storage policy of file1 is satisfied.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testWithFederationHA() throws Exception {
    try {
      conf = new HdfsConfiguration();
      final MiniDFSCluster haCluster = new MiniDFSCluster
          .Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHAFederatedTopology(2))
          .storageTypes(storageTypes)
          .numDataNodes(storageTypes.length).build();
      haCluster.waitActive();
      haCluster.transitionToActive(1);
      haCluster.transitionToActive(3);

      fs = HATestUtil.configureFailoverFs(haCluster, conf);
      createTestFiles(fs, (short) 3);

      fs.setStoragePolicy(testFile, WARM);
      fs.satisfyStoragePolicy(testFile);

      haCluster.transitionToStandby(1);
      haCluster.transitionToActive(0);
      haCluster.transitionToStandby(3);
      haCluster.transitionToActive(2);

      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.DISK, 1, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.ARCHIVE, 2, timeout, fs);

    } finally {
      clusterShutdown();
    }
  }

  /**
   * Tests to verify SPS xattr will be removed if the satisfy work has
   * been finished, expect that the method satisfyStoragePolicy can be
   * invoked on the same file again after the block movement has been
   * finished:
   * 1. satisfy storage policy of file1.
   * 2. wait until storage policy is satisfied.
   * 3. satisfy storage policy of file1 again
   * 4. make sure step 3 works as expected.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testMultipleSatisfyStoragePolicy() throws Exception {
    try {
      // Lower block movement check for testing.
      conf = new HdfsConfiguration();
      final long minCheckTimeout = 500; // minimum value
      conf.setLong(
          DFSConfigKeys.DFS_STORAGE_POLICY_SATISFIER_RECHECK_TIMEOUT_MILLIS_KEY,
          minCheckTimeout);
      clusterSetUp(conf);
      fs.setStoragePolicy(testFile, ONE_SSD);
      fs.satisfyStoragePolicy(testFile);
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.SSD, 1, timeout, fs);
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.DISK, 2, timeout, fs);

      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved(testFileName,
          XATTR_SATISFY_STORAGE_POLICY, cluster.getNamesystem(), 30000);

      fs.setStoragePolicy(testFile, COLD);
      fs.satisfyStoragePolicy(testFile);
      DFSTestUtil.waitExpectedStorageType(
          testFileName, StorageType.ARCHIVE, 3, timeout, fs);
    } finally {
      clusterShutdown();
    }
  }

  /**
   * Tests to verify SPS xattr is removed after SPS is dropped,
   * expect that if the SPS is disabled/dropped, the SPS
   * xattr should be removed accordingly:
   * 1. satisfy storage policy of file1.
   * 2. drop SPS thread in block manager.
   * 3. make sure sps xattr is removed.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testDropSPS() throws Exception {
    try {
      clusterSetUp();
      fs.setStoragePolicy(testFile, ONE_SSD);
      fs.satisfyStoragePolicy(testFile);

      cluster.getNamesystem().getBlockManager().deactivateSPS();

      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved(testFileName,
          XATTR_SATISFY_STORAGE_POLICY, cluster.getNamesystem(), 30000);

    } finally {
      clusterShutdown();
    }
  }

  /**
   * Tests that Xattrs should be cleaned if all blocks already satisfied.
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testSPSShouldNotLeakXattrIfStorageAlreadySatisfied()
      throws Exception {
    try {
      clusterSetUp();
      DFSTestUtil.waitExpectedStorageType(testFileName, StorageType.DISK, 3,
          timeout, fs);
      fs.satisfyStoragePolicy(testFile);

      DFSTestUtil.waitExpectedStorageType(testFileName, StorageType.DISK, 3,
          timeout, fs);

      // Make sure satisfy xattr has been removed.
      DFSTestUtil.waitForXattrRemoved(testFileName,
          XATTR_SATISFY_STORAGE_POLICY, cluster.getNamesystem(), 30000);

    } finally {
      clusterShutdown();
    }
  }

  /**
   * Restart the hole env and trigger the DataNode's heart beats.
   * @throws Exception
   */
  private void restartCluster() throws Exception {
    cluster.restartDataNodes();
    cluster.restartNameNodes();
    cluster.waitActive();
    cluster.triggerHeartbeats();
  }

}
