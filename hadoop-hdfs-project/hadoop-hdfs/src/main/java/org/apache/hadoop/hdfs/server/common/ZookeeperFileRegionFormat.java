/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperFileRegionFormat
    extends BlockFormat<FileRegion> implements Configurable {

  private static final Logger LOG =
      LoggerFactory.getLogger(ZookeeperFileRegionFormat.class);

  private Configuration conf;
  private ZookeeperReader.Options readerOpts = ZookeeperReader.defaults();
  private ZookeeperWriter.Options writerOpts = ZookeeperWriter.defaults();

  private static final String ZKPATH = "/provided";
  private static final String ZKPATH_ROOT = ZKPATH + "/";

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    ((Configurable)readerOpts).setConf(conf);
    ((Configurable)writerOpts).setConf(conf);
  }

  @Override
  public Configuration getConf() {
        return conf;
    }

  @Override
  public Reader<FileRegion> getReader(Reader.Options opts)
    throws IOException {
      if (null == opts) {
        opts = readerOpts;
      }
      if (!(opts instanceof ZookeeperOptions)) {
        throw new IllegalArgumentException("Invalid options " + opts.getClass());
      }
      ZookeeperOptions o = (ZookeeperOptions) opts;
      return new ZookeeperReader(o.hostPort);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    if (!(opts instanceof ZookeeperOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    ZookeeperOptions o = (ZookeeperOptions) opts;
    return new ZookeeperWriter(o.hostPort);
  }

  public static class ZookeeperOptions
    implements ZookeeperReader.Options, ZookeeperWriter.Options, Configurable {

    public static final String HOST_PORT = "hdfs.image.block.zookeeper.hostPort";

    private Configuration conf;
    protected String hostPort = null;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.hostPort = conf.getTrimmed(HOST_PORT);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public ZookeeperOptions hostPort(String hostPort) {
      this.hostPort = hostPort;
      return this;
    }
  }

  public static class ZookeeperReader extends Reader<FileRegion> implements Watcher {

    public interface Options extends Reader.Options {
      Options hostPort(String hostPort);
    }

    private ZooKeeper zk;

    // TODO need a partitioner
    public ZookeeperReader(String hostPort) {
      try {
        zk = new ZooKeeper(hostPort, 3000, this);
      } catch (IOException e) {
        LOG.error("Could not create ZooKeeper client.");
      }
    }

    public static Options defaults() {
      return new ZookeeperOptions();
    }

    @Override
    public void process(WatchedEvent we) {
    }

    @Override
    public FileRegion resolve(Block ident) throws IOException {
      try {
        String path = ZKPATH_ROOT + ident.getBlockId();
        Stat stat = zk.exists(path, false);
        if (null == stat) {
          throw new IOException("Could not find znode: " + path);
        }
        byte[] data = zk.getData(path, false, stat);
        String block = new String(data);
        return stringToFileRegion(block);
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public Iterator<FileRegion> iterator() {
      try {
        List<String> children = zk.getChildren(ZKPATH, false);
        return new ZKCrawler(zk, children);
      } catch (KeeperException | InterruptedException e) {
      }
      return Collections.emptyIterator();
    }

    @Override
    public void close() throws IOException {
      // empty
    }

  }

  public static class ZookeeperWriter extends Writer<FileRegion> implements Watcher {

    public interface Options extends Writer.Options {
      Options hostPort(String hostPort);
    }

    public static Options defaults() {
      return new ZookeeperOptions();
    }

    private ZooKeeper zk;

    public ZookeeperWriter(String hostPort) {
      try {
        zk = new ZooKeeper(hostPort, 3000, this);
        final Stat stat = zk.exists(ZKPATH, false);
        if (null == stat)  {
          zk.create(ZKPATH, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
      } catch (IOException | KeeperException | InterruptedException e) {
        LOG.error("Could not create ZooKeeper client: {}", e);
      }
    }

    @Override
    public void process(WatchedEvent we) {

    }

    @Override
    public void store(FileRegion alias) throws IOException {
      try {
        final String path = ZKPATH_ROOT + alias.getBlock().getBlockId();
        final Stat stat = zk.exists(path, false);
        StringBuilder content = new StringBuilder();
        content.append(Long.toString(alias.getBlock().getBlockId()));
        content.append(',');
        content.append(alias.getPath());
        content.append(',');
        content.append(Long.toString(alias.getOffset()));
        content.append(',');
        content.append(Long.toString(alias.getLength()));
        byte[] data = content.toString().getBytes(Charset.forName("UTF-8"));

        if (null == stat) {
          zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
          zk.setData(path, data, stat.getVersion());
        }
      } catch (KeeperException | InterruptedException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // empty
    }

  }

  public static class ZKCrawler implements Iterator<FileRegion>{
    private final ZooKeeper zk;
    private Iterator<String> curr;
    ZKCrawler(ZooKeeper zk, List<String> nodes) {
      this.zk = zk;
      this.curr = nodes.iterator();
    }

    @Override
    public FileRegion next() {
      try {
        String blockId = curr.next();
        String path = ZKPATH_ROOT + blockId;
        Stat stat = zk.exists(path, false);
        if (null == stat) {
          LOG.warn("Block id does not exist: {}. Trying to skip ahead.", blockId);
          if (curr.hasNext()) {
            return next();
          }
          return null;
        }
        byte[] data = zk.getData(path, false, stat);
        return stringToFileRegion(new String(data));
      } catch (KeeperException | InterruptedException e) {
      }
      throw new RuntimeException("Could not create a FileRegion.");
    }

    @Override
    public boolean hasNext() {
      return curr.hasNext();
    }
    @Override
    public void remove() {
      curr.remove();
    }
  }

  /**
   *
   * @param s - Comma delimited string of the Provided storage mapping tuple
   * @return FileRegion
   */
  static FileRegion stringToFileRegion(String s) {
    String[] blockSplit = s.split(",");
    return new FileRegion(Long.parseLong(blockSplit[0]),
        new Path(blockSplit[1]), Long.parseLong(blockSplit[2]), Long.parseLong(blockSplit[3]));
  }
}
