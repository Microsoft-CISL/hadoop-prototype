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
import java.net.URISyntaxException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.io.IOUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;

import org.apache.hadoop.fs.s3a.S3AFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3AFileRegionFormat
        extends BlockFormat<FileRegion> implements Configurable {

  private static final Logger LOG
          = LoggerFactory.getLogger(S3AFileRegionFormat.class);

  private Configuration conf;
  private S3AReader.Options readerOpts = S3AReader.defaults();
  private S3AWriter.Options writerOpts = S3AWriter.defaults();
  private S3AFileSystem fs;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    ((Configurable) readerOpts).setConf(conf);
    ((Configurable) writerOpts).setConf(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private S3AFileSystem createClient(S3AOptions o) throws IOException {
    if (null == o.account || null == o.key || null == o.bucket) {
      throw new IOException("Incomplete credentials");
    }
    if (fs != null) {
      return fs;
    }
    try {
      S3AFileSystem newFs = new S3AFileSystem();
      newFs.initialize(new URI(o.bucket), conf);
      fs = newFs;
      return fs;
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Reader<FileRegion> getReader(Reader.Options opts) throws IOException {
    if (null == opts) {
      opts = readerOpts;
    }
    if (!(opts instanceof S3AOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    S3AOptions o = (S3AOptions) opts;
    return new S3AReader(createClient(o), o.partition);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    if (!(opts instanceof S3AOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    S3AOptions o = (S3AOptions) opts;
    return new S3AWriter(createClient(o), o.partition);
  }

  public static class S3AOptions
          implements S3AReader.Options, S3AWriter.Options, Configurable {

    public static final String ACCOUNT = "hdfs.image.block.s3a.account";
    public static final String KEY = "hdfs.image.block.s3a.key";
    public static final String BUCKET = "hdfs.image.block.s3a.bucket";
    public static final String PARTITION = "hdfs.image.block.s3a.partition";

    private Configuration conf;
    protected String key = null;
    protected String bucket = null;
    protected String account = null;
    protected String partition = null;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.key = conf.getTrimmed(KEY);
      this.bucket = conf.getTrimmed(BUCKET);
      this.account = conf.getTrimmed(ACCOUNT);
      this.partition = conf.getTrimmed(PARTITION);
      this.partition = "/blocks.csv"; //temporary hardcoded
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public S3AOptions account(String account) {
      this.account = account;
      return this;
    }

    @Override
    public S3AOptions key(String key) {
      this.key = key;
      return this;
    }

    @Override
    public S3AOptions bucket(String bucket) {
      this.bucket = bucket;
      return this;
    }

    @Override
    public S3AOptions partition(String partition) {
      this.partition = partition;
      return this;
    }

  }

  public static class S3AReader extends Reader<FileRegion> {

    public interface Options extends Reader.Options {

      Options account(String account);

      Options key(String key);

      Options bucket(String bucket);

      Options partition(String ptn);
    }

    private final S3AFileSystem fs;
    private final String partition;

    // TODO need a partitioner
    S3AReader(S3AFileSystem fs, String partition) {
      this.fs = fs;
      this.partition = partition;
    }

    public static Options defaults() {
      return new S3AOptions();
    }

    @Override
    public FileRegion resolve(Block ident) throws IOException {
      HashMap<Long, FileRegion> map = readBlockTable(fs, fs.makeQualified(new Path(partition)));
      if (map.containsKey(ident.getBlockId())) {
        return map.get(ident.getBlockId());
      }
      throw new IOException("Block " + ident + " not found on provided storage");
    }

    @Override
    public Iterator<FileRegion> iterator() {
      try {
        HashMap<Long, FileRegion> map = readBlockTable(fs, fs.makeQualified(new Path(partition)));
        return map.values().iterator();
      } catch (IOException ex) {
        //log
        return Collections.emptyListIterator();
      }
    }

    @Override
    public void close() throws IOException {
      // empty
    }

  }

  public static class S3AWriter extends Writer<FileRegion> {

    public interface Options extends Writer.Options {

      Options account(String account);

      Options key(String key);

      Options bucket(String bucket);

      Options partition(String partition);
    }

    public static Options defaults() {
      return new S3AOptions();
    }

    private final S3AFileSystem fs;
    private final String partition;

    S3AWriter(S3AFileSystem fs, String partition) {
      this.fs = fs;
      this.partition = partition;
    }

    @Override
    public void store(FileRegion alias) throws IOException {
      LOG.info("storing " + alias.toString());
      //TODO buffer, write file on close?
      HashMap<Long, FileRegion> map = readBlockTable(fs,
              fs.makeQualified(new Path(partition)));
      map.put(alias.blockId, alias);
      blockTableToFile(fs, fs.makeQualified(new Path(partition)), map);
    }

    @Override
    public void close() throws IOException {
      // empty
    }

  }

  private static HashMap<Long, FileRegion> readBlockTable(S3AFileSystem fs, Path path) throws IOException {
    String stringValue;
    try (FSDataInputStream inputStream = fs.open(path)) {
      stringValue = IOUtils.toString(inputStream);
    }
    HashMap<Long, FileRegion> map = new HashMap();
    String[] blocks = stringValue.split("\n");
    for (String block : blocks) {
      String[] blockSplit = block.split(",");
      Path qualified = fs.makeQualified(
              Path.getPathWithoutSchemeAndAuthority(new Path(blockSplit[1])));
      LOG.info("read path " + qualified + " for input " + blockSplit[1]);
      map.put(Long.parseLong(blockSplit[0]), new FileRegion(Long.parseLong(blockSplit[0]),
              qualified,
              Long.parseLong(blockSplit[2]), Long.parseLong(blockSplit[3])));
    }
    return map;
  }

  private static void blockTableToFile(S3AFileSystem fs, Path path,
          HashMap<Long, FileRegion> map) throws IOException {
    try (FSDataOutputStream outputStream = fs.create(path, true)) {
      StringBuilder content = new StringBuilder();
      for (Long index : map.keySet()) {
        FileRegion region = map.get(index);
        content.append(Long.toString(index));
        content.append(',');
        content.append(fs.makeQualified(
                Path.getPathWithoutSchemeAndAuthority(region.path)));
        content.append(',');
        content.append(Long.toString(region.offset));
        content.append(',');
        content.append(Long.toString(region.length));
        content.append('\n');
      }
      outputStream.write(content.toString().getBytes(Charset.forName("UTF-8")));
    }
  }
}
