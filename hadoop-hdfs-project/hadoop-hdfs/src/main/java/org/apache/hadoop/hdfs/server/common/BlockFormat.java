package org.apache.hadoop.hdfs.server.common;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hdfs.protocol.Block;

public abstract class BlockFormat<T extends BlockAlias>  {

  public static abstract class Reader<U extends BlockAlias>
      implements Iterable<U>, Closeable {
    public interface Options { }

    public abstract U resolve(Block ident) throws IOException;

  }

  public abstract Reader<T> getReader(Reader.Options opts) throws IOException;

  public static abstract class Writer<U extends BlockAlias>
      implements Closeable {
    public interface Options { }

    public abstract void store(U token) throws IOException;

  }

  public abstract Writer<T> getWriter(Writer.Options opts) throws IOException;

}
