package org.apache.hadoop.hdfs.server.namenode;

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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MultipleIOException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import com.google.common.annotations.VisibleForTesting;

// TODO: rename to CSVFileRegionFormat
// TODO: refactor to abstract file format
// TODO: move delimiter to common
public class CSVBlockFormat extends BlockFormat<FileRegion> {

  public static final String DEFNAME   = "blocks.csv";
  public static final String DELIMITER = ",";

  @Override
  public Reader<FileRegion> getReader(Reader.Options opts)
      throws IOException {
    if (!(opts instanceof ReaderOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    ReaderOptions o = (ReaderOptions) opts;
    Configuration conf = null == o.getConf()
      ? new Configuration()
      : o.getConf();
    return createReader(o.file, o.delim, conf);
  }

  @VisibleForTesting
  CSVReader createReader(Path file, String delim, Configuration conf)
      throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(file);
    return new CSVReader(fs, file, codec, delim);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    if (!(opts instanceof WriterOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    WriterOptions o = (WriterOptions) opts;
    Configuration conf = (null == o.getConf())
      ? new Configuration()
      : o.getConf();
    if (o.codec != null) {
      CompressionCodecFactory factory = new CompressionCodecFactory(conf);
      CompressionCodec codec = factory.getCodecByName(o.codec);
      String name = o.file.getName() + codec.getDefaultExtension();
      o.filename(new Path(o.file.getParent(), name));
      return createWriter(o.file, codec, o.delim, conf);
    }
    return createWriter(o.file, null, o.delim, conf);
  }

  @VisibleForTesting
  CSVWriter createWriter(Path file, CompressionCodec codec, String delim,
      Configuration conf) throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    OutputStream tmp = fs.create(file);
    java.io.Writer out = new BufferedWriter(new OutputStreamWriter(
          (null == codec) ? tmp : codec.createOutputStream(tmp), "UTF-8"));
    return new CSVWriter(out, delim);
  }

  public static class ReaderOptions implements CSVReader.Options, Configurable {

    public static final String FILEPATH = "hdfs.image.block.csv.read.path";
    public static final String DELIMITER = "hdfs.image.block.csv.delimiter";

    private Configuration conf;
    protected String delim = DELIMITER;
    protected Path file = new Path(new File(DEFNAME).toURI().toString());

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String tmpfile = conf.get(FILEPATH);
      file = new Path(tmpfile);
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

  public static class WriterOptions implements CSVWriter.Options, Configurable {

    public static final String CODEC     = "hdfs.image.block.csv.read.codec";
    public static final String FILEPATH  = "hdfs.image.block.csv.write.path";
    public static final String DELIMITER = "hdfs.image.block.csv.delimiter";

    private Configuration conf;
    protected String codec = null;
    protected Path file = new Path(new File(DEFNAME).toURI().toString());
    protected String delim = DELIMITER;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      String tmpfile = conf.get(FILEPATH);
      file = new Path(tmpfile);
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

  static class CSVReader extends Reader<FileRegion> {

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
    private final Map<FRIterator,BufferedReader> iterators;

    protected CSVReader(FileSystem fs, Path file, CompressionCodec codec,
        String delim) {
      this(fs, file, codec, delim,
          new IdentityHashMap<FRIterator,BufferedReader>());
    }

    CSVReader(FileSystem fs, Path file, CompressionCodec codec, String delim,
        Map<FRIterator,BufferedReader> iterators) {
      this.fs = fs;
      this.file = file;
      this.codec = codec;
      this.delim = delim;
      this.iterators = Collections.synchronizedMap(iterators);
    }

    public FileRegion resolve(String ident) throws IOException {
      // consider layering index w/ composable format
      Iterator<FileRegion> i = iterator();
      try {
        while (i.hasNext()) {
          FileRegion f = i.next();
          if (f.getBlockId().equals(ident)) {
            return f;
          }
        }
      } finally {
        BufferedReader r = iterators.remove(i);
        r.close();
      }
      return null;
    }

    class FRIterator implements Iterator<FileRegion> {
      FileRegion pending;

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
      if (f.length != 4) {
        throw new IOException("Invalid line: " + line);
      }
      return new FileRegion(f[0], new Path(f[1]),
          Long.valueOf(f[2]), Long.valueOf(f[3]));
    }

    InputStream createStream() throws IOException {
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
          new BufferedReader(new InputStreamReader(createStream()));
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

  static class CSVWriter extends Writer<FileRegion> {

    final String delim;
    final java.io.Writer out;

    protected CSVWriter(java.io.Writer out, String delim) {
      this.out = out;
      this.delim = delim;
    }

    @Override
    public void store(FileRegion token) throws IOException {
      out.append(token.getBlockId()).append(delim);
      out.append(token.path.toString()).append(delim);
      out.append(Long.toString(token.offset)).append(delim);
      out.append(Long.toString(token.length)).append("\n");
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    public static Options defaults() {
      return new WriterOptions();
    }

    public interface Options extends Writer.Options {
      Options codec(String codec);
      Options filename(Path file);
      Options delimiter(String delim);
    }

  }

}
