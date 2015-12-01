package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;

import static org.apache.hadoop.hdfs.server.namenode.TextFileRegionFormat.*;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestTextBlockFormat {

  static final Path OUTFILE = new Path("yak://dingo:4344/bar/baz");

  void check(TextWriter.Options opts, final Path vp,
      final Class<? extends CompressionCodec> vc) throws IOException {
    TextFileRegionFormat mFmt = new TextFileRegionFormat() {
      @Override
      TextWriter createWriter(Path file, CompressionCodec codec, String delim,
          Configuration conf) throws IOException {
        assertEquals(vp, file);
        if (null == vc) {
          assertNull(codec);
        } else {
          assertEquals(vc, codec.getClass());
        }
        return null; // ignored
      }
    };
    mFmt.getWriter(opts);
  }

  @Test
  public void testWriterOptions() throws Exception {
    TextWriter.Options opts = TextWriter.defaults();
    assertTrue(opts instanceof WriterOptions);
    WriterOptions wopts = (WriterOptions) opts;
    Path curdir = new Path(new File(".").toURI().toString());
    Path def = new Path(curdir, DEFNAME);
    assertEquals(def, wopts.file);
    assertNull(wopts.codec);

    opts.filename(OUTFILE);
    check(opts, OUTFILE, null);

    opts.filename(OUTFILE);
    opts.codec("gzip");
    Path cp = new Path(OUTFILE.getParent(), OUTFILE.getName() + ".gz");
    check(opts, cp, org.apache.hadoop.io.compress.GzipCodec.class);

  }

  @Test
  public void testCSVReadWrite() throws Exception {
    final DataOutputBuffer out = new DataOutputBuffer();
    FileRegion r1 = new FileRegion("blk_4344", OUTFILE, 0, 1024);
    FileRegion r2 = new FileRegion("blk_4345", OUTFILE, 1024, 1024);
    FileRegion r3 = new FileRegion("blk_4346", OUTFILE, 2048, 512);
    try (TextWriter csv = new TextWriter(new OutputStreamWriter(out), ",")) {
      csv.store(r1);
      csv.store(r2);
      csv.store(r3);
    }
    Iterator<FileRegion> i3;
    try (TextReader csv = new TextReader(null, null, null, ",") {
      @Override
      InputStream createStream() {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), 0, out.getLength());
        return in;
      }}) {
      Iterator<FileRegion> i1 = csv.iterator();
      assertEquals(r1, i1.next());
      Iterator<FileRegion> i2 = csv.iterator();
      assertEquals(r1, i2.next());
      assertEquals(r2, i2.next());
      assertEquals(r3, i2.next());
      assertEquals(r2, i1.next());
      assertEquals(r3, i1.next());

      assertFalse(i1.hasNext());
      assertFalse(i2.hasNext());
      i3 = csv.iterator();
    }
    try {
      i3.next();
    } catch (IllegalStateException e) {
      return;
    }
    fail("Invalid iterator");
  }

  @Test
  public void testCSVReadWriteTsv() throws Exception {
    final DataOutputBuffer out = new DataOutputBuffer();
    FileRegion r1 = new FileRegion("blk_4344", OUTFILE, 0, 1024);
    FileRegion r2 = new FileRegion("blk_4345", OUTFILE, 1024, 1024);
    FileRegion r3 = new FileRegion("blk_4346", OUTFILE, 2048, 512);
    try (TextWriter csv = new TextWriter(new OutputStreamWriter(out), "\t")) {
      csv.store(r1);
      csv.store(r2);
      csv.store(r3);
    }
    Iterator<FileRegion> i3;
    try (TextReader csv = new TextReader(null, null, null, "\t") {
      @Override
      InputStream createStream() {
        DataInputBuffer in = new DataInputBuffer();
        in.reset(out.getData(), 0, out.getLength());
        return in;
      }}) {
      Iterator<FileRegion> i1 = csv.iterator();
      assertEquals(r1, i1.next());
      Iterator<FileRegion> i2 = csv.iterator();
      assertEquals(r1, i2.next());
      assertEquals(r2, i2.next());
      assertEquals(r3, i2.next());
      assertEquals(r2, i1.next());
      assertEquals(r3, i1.next());

      assertFalse(i1.hasNext());
      assertFalse(i2.hasNext());
      i3 = csv.iterator();
    }
    try {
      i3.next();
    } catch (IllegalStateException e) {
      return;
    }
    fail("Invalid iterator");
  }

}
