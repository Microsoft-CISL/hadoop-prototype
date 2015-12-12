package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.hdfs.server.common.BlockFormat.Reader.Options;
import org.apache.hadoop.hdfs.server.common.FileRegion;

public class NullBlockFormat extends BlockFormat<FileRegion> {

  @Override
  public Reader<FileRegion> getReader(Options opts) throws IOException {
    return new Reader<FileRegion>() {
      @Override
      public Iterator<FileRegion> iterator() {
        return new Iterator<FileRegion>() {
          @Override
          public boolean hasNext() {
            return false;
          }
          @Override
          public FileRegion next() {
            throw new NoSuchElementException();
          }
          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }

      @Override
      public void close() throws IOException {
        // do nothing
      }

      @Override
      public FileRegion resolve(Block ident) throws IOException {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    return new Writer<FileRegion>() {
      @Override
      public void store(FileRegion token) throws IOException {
        // do nothing
      }

      @Override
      public void close() throws IOException {
        // do nothing
      }
    };
  }

}
