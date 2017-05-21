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

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * This class is used for block maps stored as text files,
 * with a specified delimiter.
 */
public class TextFileRegionFormat
    extends BlockFormat<FileRegion> implements Configurable {

  private Configuration conf;
  private ReaderOptions readerOpts = TextReader.defaults();
  private WriterOptions writerOpts = TextWriter.defaults();

  private Path basePath;
  public static final Logger LOG =
      LoggerFactory.getLogger(TextFileRegionFormat.class);

  @Override
  public void setConf(Configuration conf) {
    readerOpts.setConf(conf);
    writerOpts.setConf(conf);
    this.conf = conf;
    basePath = new Path(conf.get(DFSConfigKeys.DFS_PROVIDER_BLOCK_MAP_BASE_URI,
        DFSConfigKeys.DFS_PROVIDER_BLOCK_MAP_BASE_URI_DEFAULT));
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public FileRegion newRegion(ExtendedBlock eb) {
    //TODO how to determine the right path and the right offset for this region?
    // for now every block will have it's own file in the base path!!
    return new FileRegion(eb.getBlockId(),
        new Path(basePath, eb.getBlockPoolId() + "-" + eb.getBlockId()), 0,
        eb.getNumBytes(), eb.getBlockPoolId(), eb.getGenerationStamp());
  }

  @Override
  public Reader<FileRegion> getReader(Reader.Options opts)
      throws IOException {
    if (null == opts) {
      opts = readerOpts;
    }
    if (!(opts instanceof ReaderOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    ReaderOptions o = (ReaderOptions) opts;
    Configuration readerConf = (null == o.getConf())
        ? new Configuration()
            : o.getConf();
    return createReader(o.file, o.delim, readerConf);
  }

  @VisibleForTesting
  TextReader createReader(Path file, String delim, Configuration cfg)
      throws IOException {
    FileSystem fs = file.getFileSystem(cfg);
    if (fs instanceof LocalFileSystem) {
      fs = ((LocalFileSystem)fs).getRaw();
    }
    CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
    CompressionCodec codec = factory.getCodec(file);
    return new TextReader(fs, file, codec, delim);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    return  getWriter(opts, false);
  }

  private Writer<FileRegion> getWriter(Writer.Options opts, boolean append)
      throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    if (!(opts instanceof WriterOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    WriterOptions o = (WriterOptions) opts;
    Configuration cfg = (null == o.getConf())
            ? new Configuration()
            : o.getConf();
    if (o.codec != null) {
      CompressionCodecFactory factory = new CompressionCodecFactory(cfg);
      CompressionCodec codec = factory.getCodecByName(o.codec);
      String name = o.file.getName() + codec.getDefaultExtension();
      o.filename(new Path(o.file.getParent(), name));
      return createWriter(o.file, codec, o.delim, cfg, append);
    }
    return createWriter(o.file, null, o.delim, conf, append);
  }

  public Writer<FileRegion> append(Writer.Options opts) throws IOException {
    return getWriter(opts, true);
  }

  @VisibleForTesting
  TextWriter createWriter(Path file, CompressionCodec codec, String delim,
      Configuration cfg, boolean append) throws IOException {
    FileSystem fs = file.getFileSystem(cfg);
    if (fs instanceof LocalFileSystem) {
      fs = ((LocalFileSystem)fs).getRaw();
    }

    OutputStream tmp;
    if (append) {
      tmp = fs.append(file);
    } else {
      tmp = fs.create(file);
    }
    java.io.Writer out = new BufferedWriter(new OutputStreamWriter(
          (null == codec) ? tmp : codec.createOutputStream(tmp), "UTF-8"));
    return new TextWriter(out, delim);
  }

  /**
   * Class specifying reader options for the {@link TextFileRegionFormat}.
   */
  public static class ReaderOptions
      implements TextReader.Options, Configurable {

    private Configuration conf;
    private String delim =
        DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER_DEFAULT;
    private Path file = new Path(
        new File(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_PATH_DEFAULT)
        .toURI().toString());

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String tmpfile = conf.get(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_READ_PATH,
          DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_PATH_DEFAULT);
      file = new Path(tmpfile);
      delim = conf.get(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER,
          DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER_DEFAULT);
      LOG.info("TextFileRegionFormat: read path " + tmpfile.toString());
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public ReaderOptions filename(Path file) {
      this.file = file;
      return this;
    }

    @Override
    public ReaderOptions delimiter(String delim) {
      this.delim = delim;
      return this;
    }
  }

  /**
   * Class specifying writer options for the {@link TextFileRegionFormat}.
   */
  public static class WriterOptions
      implements TextWriter.Options, Configurable {

    private Configuration conf;
    private String codec = null;
    private Path file =
        new Path(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_PATH_DEFAULT);
    private String delim =
        DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER_DEFAULT;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String tmpfile = conf.get(
          DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_WRITE_PATH, file.toString());
      file = new Path(tmpfile);
      codec = conf.get(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_CODEC);
      delim = conf.get(DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER,
          DFSConfigKeys.DFS_PROVIDED_BLOCK_MAP_DELIMITER_DEFAULT);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public WriterOptions filename(Path file) {
      this.file = file;
      return this;
    }

    public String getCodec() {
      return codec;
    }

    public Path getFile() {
      return file;
    }

    @Override
    public WriterOptions codec(String codec) {
      this.codec = codec;
      return this;
    }

    @Override
    public WriterOptions delimiter(String delim) {
      this.delim = delim;
      return this;
    }

  }

  /**
   * This class is used as a reader for block maps which
   * are stored as delimited text files.
   */
  public static class TextReader extends Reader<FileRegion> {

    /**
     * Options for {@link TextReader}.
     */
    public interface Options extends Reader.Options {
      Options filename(Path file);
      Options delimiter(String delim);
    }

    static ReaderOptions defaults() {
      return new ReaderOptions();
    }

    private final Path file;
    private final String delim;
    private final FileSystem fs;
    private final CompressionCodec codec;
    private final Map<FRIterator, BufferedReader> iterators;

    protected TextReader(FileSystem fs, Path file, CompressionCodec codec,
        String delim) {
      this(fs, file, codec, delim,
          new IdentityHashMap<FRIterator, BufferedReader>());
    }

    TextReader(FileSystem fs, Path file, CompressionCodec codec, String delim,
        Map<FRIterator, BufferedReader> iterators) {
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.delim = delim;
      this.iterators = Collections.synchronizedMap(iterators);
    }

    @Override
    public FileRegion resolve(Block ident) throws IOException {
      // consider layering index w/ composable format
      Iterator<FileRegion> i = iterator();
      try {
        while (i.hasNext()) {
          FileRegion f = i.next();
          if (f.getBlock().equals(ident)) {
            return f;
          }
        }
      } finally {
        BufferedReader r = iterators.remove(i);
        if (r != null) {
          // null on last element
          r.close();
        }
      }
      return null;
    }

    class FRIterator implements Iterator<FileRegion> {

      private FileRegion pending;

      @Override
      public boolean hasNext() {
        return pending != null;
      }

      @Override
      public FileRegion next() {
        if (null == pending) {
          throw new NoSuchElementException();
        }
        FileRegion ret = pending;
        try {
          pending = nextInternal(this);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return ret;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }

    private FileRegion nextInternal(Iterator<FileRegion> i) throws IOException {
      BufferedReader r = iterators.get(i);
      if (null == r) {
        throw new IllegalStateException();
      }
      String line = r.readLine();
      if (null == line) {
        iterators.remove(i);
        return null;
      }
      String[] f = line.split(delim);
      if (f.length != 6) {
        throw new IOException("Invalid line: " + line);
      }
      return new FileRegion(Long.parseLong(f[0]), new Path(f[1]),
          Long.parseLong(f[2]), Long.parseLong(f[3]), f[5],
          Long.parseLong(f[4]));
    }

    public InputStream createStream() throws IOException {
      InputStream i = fs.open(file);
      if (codec != null) {
        i = codec.createInputStream(i);
      }
      return i;
    }

    @Override
    public Iterator<FileRegion> iterator() {
      FRIterator i = new FRIterator();
      try {
        BufferedReader r =
            new BufferedReader(new InputStreamReader(createStream(), "UTF-8"));
        iterators.put(i, r);
        i.pending = nextInternal(i);
      } catch (IOException e) {
        iterators.remove(i);
        throw new RuntimeException(e);
      }
      return i;
    }

    @Override
    public void close() throws IOException {
      ArrayList<IOException> ex = new ArrayList<>();
      synchronized (iterators) {
        for (Iterator<BufferedReader> i = iterators.values().iterator();
             i.hasNext();) {
          try {
            BufferedReader r = i.next();
            r.close();
          } catch (IOException e) {
            ex.add(e);
          } finally {
            i.remove();
          }
        }
        iterators.clear();
      }
      if (!ex.isEmpty()) {
        throw MultipleIOException.createIOException(ex);
      }
    }

  }

  /**
   * This class is used as a writer for block maps which
   * are stored as delimited text files.
   */
  public static class TextWriter extends Writer<FileRegion> {

    /**
     * Interface for Writer options.
     */
    public interface Options extends Writer.Options {
      Options codec(String codec);
      Options filename(Path file);
      Options delimiter(String delim);
    }

    public static WriterOptions defaults() {
      return new WriterOptions();
    }

    private final String delim;
    private final java.io.Writer out;

    public TextWriter(java.io.Writer out, String delim) {
      this.out = out;
      this.delim = delim;
    }

    @Override
    public void store(FileRegion token) throws IOException {
      out.append(String.valueOf(token.getBlock().getBlockId())).append(delim);
      out.append(token.getPath().toString()).append(delim);
      out.append(Long.toString(token.getOffset())).append(delim);
      out.append(Long.toString(token.getLength())).append(delim);
      out.append(Long.toString(token.getGenerationStamp())).append(delim);
      out.append(token.getBlockPoolId()).append("\n");
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

  }

  @Override
  public void refresh() throws IOException {
    //nothing to do;
  }

}
