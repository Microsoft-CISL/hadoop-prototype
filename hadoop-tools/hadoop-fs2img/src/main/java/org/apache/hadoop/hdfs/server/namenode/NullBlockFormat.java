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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.hdfs.server.common.BlockFormat.Reader.Options;
import org.apache.hadoop.hdfs.server.common.FileRegion;

/**
 * Null sink for region information emitted from FSImage.
 */
public class NullBlockFormat extends BlockFormat<FileRegion> {

  @Override
  public FileRegion newRegion(ExtendedBlock eb) {
    return null;
  }

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

  @Override
  public Writer<FileRegion> append(Writer.Options opts) throws IOException {
    return getWriter(opts);
  }

  @Override
  public void refresh() throws IOException {
    // do nothing
  }

}
