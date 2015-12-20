package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.Block;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableResult;
import com.microsoft.azure.storage.table.TableServiceEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureFileRegionFormat
    extends BlockFormat<FileRegion> implements Configurable {

  private static final Logger LOG =
    LoggerFactory.getLogger(AzureFileRegionFormat.class);

  private Configuration conf;
  private AzureReader.Options readerOpts = AzureReader.defaults();
  private AzureWriter.Options writerOpts = AzureWriter.defaults();

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
    if (!(opts instanceof AzureOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    AzureOptions o = (AzureOptions) opts;
    if (null == o.account) {
      throw new RuntimeException("Missing account");
    }
    if (null == o.key) {
      throw new RuntimeException("Missing key");
    }
    if (null == o.table) {
      throw new RuntimeException("Missing table");
    }
    try {
      CloudStorageAccount account = new CloudStorageAccount(
        new StorageCredentialsAccountAndKey(o.account, o.key), true);

      CloudTableClient client = account.createCloudTableClient();
      CloudTable tbl = client.getTableReference(o.table);
      return new AzureReader(tbl);
      //tbl.createIfNotExists();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (StorageException e) {
      throw new RuntimeException("Could not create \"" + o.table + "\"", e);
    }
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    return null;
  }

  public static class AzureOptions
      implements AzureReader.Options, AzureWriter.Options, Configurable {

    public static final String ACCOUNT = "hdfs.image.block.azure.account";
    public static final String KEY     = "hdfs.image.block.azure.key";
    public static final String TABLE   = "hdfs.image.block.azure.table";

    private Configuration conf;
    protected String key = null;
    protected String table = null;
    protected String account = null;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.key = conf.get(KEY);
      this.table = conf.get(TABLE);
      this.account = conf.get(ACCOUNT);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public AzureOptions account(String account) {
      this.account = account;
      return this;
    }

    @Override
    public AzureOptions key(String key) {
      this.key = key;
      return this;
    }

    @Override
    public AzureOptions table(String name) {
      this.table = name;
      return this;
    }

  }

  public static class AzureReader extends Reader<FileRegion> {

    public interface Options extends Reader.Options {
      Options account(String account);
      Options key(String key);
      Options table(String name);
    }

    private final CloudTable tbl;

    AzureReader(CloudTable tbl) {
      this.tbl = tbl;
    }

    public static Options defaults() {
      return new AzureOptions();
    }

    @Override
    public FileRegion resolve(Block ident) throws IOException {
      return null; // TODO
    }

    @Override
    public Iterator<FileRegion> iterator() {
      return null; // TODO
    }

    @Override
    public void close() throws IOException {
    }

  }

  public static class AzureWriter extends Writer<FileRegion> {

    public interface Options extends Writer.Options {
      Options account(String account);
      Options key(String key);
      Options table(String name);
    }

    public static Options defaults() {
      return new AzureOptions();
    }

    private final CloudTable tbl;

    AzureWriter(CloudTable tbl) {
      this.tbl = tbl;
    }

    @Override
    public void store(FileRegion token) throws IOException {
      try {
        RegionEntry pe = new RegionEntry(token);
        TableOperation op = TableOperation.insert(pe);
        TableResult ins = tbl.execute(op);
        LOG.debug("Insert: {}", ins.getHttpStatusCode());
      } catch (StorageException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
    }

  }

  static class RegionEntry extends TableServiceEntity {
    private FileRegion alias;
    public RegionEntry() {
    }
    RegionEntry(FileRegion alias) {
      this.alias = alias;
    }
    @Override
    public String getPartitionKey() {
      return alias.getPath().toUri().getAuthority();
    }
    @Override
    public String getRowKey() {
      return String.valueOf(alias.getBlock().getBlockId());
    }
    @Override
    public String toString() {
      return alias.toString();
    }
  }

}
