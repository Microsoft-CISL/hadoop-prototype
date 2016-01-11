package org.apache.hadoop.hdfs.server.common;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.Block;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
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

  private CloudTable createClient(AzureOptions o) throws IOException {
    if (null == o.account || null == o.key || null == o.table) {
      throw new IOException("Incomplete credentials");
    }
    try {
      CloudStorageAccount account = new CloudStorageAccount(
        new StorageCredentialsAccountAndKey(o.account, o.key), true);

      CloudTableClient client = account.createCloudTableClient();
      CloudTable tbl = client.getTableReference(o.table);
      return tbl;
      //tbl.createIfNotExists();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    } catch (StorageException e) {
      throw new RuntimeException("Could not create \"" + o.table + "\"", e);
    }
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
    return new AzureReader(createClient(o), o.partition);
  }

  @Override
  public Writer<FileRegion> getWriter(Writer.Options opts) throws IOException {
    if (null == opts) {
      opts = writerOpts;
    }
    if (!(opts instanceof AzureOptions)) {
      throw new IllegalArgumentException("Invalid options " + opts.getClass());
    }
    AzureOptions o = (AzureOptions) opts;
    return new AzureWriter(createClient(o), o.partition);
  }

  public static class AzureOptions
      implements AzureReader.Options, AzureWriter.Options, Configurable {

    public static final String ACCOUNT   = "hdfs.image.block.azure.account";
    public static final String KEY       = "hdfs.image.block.azure.key";
    public static final String TABLE     = "hdfs.image.block.azure.table";
    public static final String PARTITION = "hdfs.image.block.azure.partition";

    private Configuration conf;
    protected String key = null;
    protected String table = null;
    protected String account = null;
    protected String partition = null;

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
      this.key = conf.get(KEY);
      this.table = conf.get(TABLE);
      this.account = conf.get(ACCOUNT);
      this.partition = conf.get(PARTITION);
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
    public AzureOptions table(String table) {
      this.table = table;
      return this;
    }

    @Override
    public AzureOptions partition(String partition) {
      this.partition = partition;
      return this;
    }

  }

  public static class AzureReader extends Reader<FileRegion> {

    public interface Options extends Reader.Options {
      Options account(String account);
      Options key(String key);
      Options table(String table);
      Options partition(String ptn);
    }

    private final CloudTable tbl;
    private final String partition;

    // TODO need a partitioner
    AzureReader(CloudTable tbl, String partition) {
      this.tbl = tbl;
      this.partition = partition;
    }

    public static Options defaults() {
      return new AzureOptions();
    }

    @Override
    public FileRegion resolve(Block ident) throws IOException {
      try {
        TableResult sel = tbl.execute(
            TableOperation.retrieve(partition,
              String.valueOf(ident.getBlockId()), RegionEntry.class));
        LOG.debug("Select: {}", sel.getHttpStatusCode());
        RegionEntry e = sel.getResultAsType();
        return e.getAlias();
      } catch (StorageException e) {
        throw new IOException(e);
      }
    }

    @Override
    public Iterator<FileRegion> iterator() {
      Iterable<RegionEntry> i = tbl.execute(TableQuery
          .from(RegionEntry.class)
          .where(TableQuery.generateFilterCondition(
              "PartitionKey", QueryComparisons.EQUAL, partition)));
      final Iterator<RegionEntry> inner = i.iterator();
      return new Iterator<FileRegion>() {
        @Override
        public FileRegion next() {
          return inner.next().getAlias();
        }
        @Override
        public boolean hasNext() {
          return inner.hasNext();
        }
        @Override
        public void remove() {
          inner.remove();
        }
      };
    }

    @Override
    public void close() throws IOException {
      // empty
    }

  }

  public static class AzureWriter extends Writer<FileRegion> {

    public interface Options extends Writer.Options {
      Options account(String account);
      Options key(String key);
      Options table(String table);
      Options partition(String partition);
    }

    public static Options defaults() {
      return new AzureOptions();
    }

    private final CloudTable tbl;
    private final String partition;

    AzureWriter(CloudTable tbl, String partition) {
      this.tbl = tbl;
      this.partition = partition;
    }

    @Override
    public void store(FileRegion alias) throws IOException {
      try {
        RegionEntry pe = new RegionEntry(alias, partition);
        TableOperation op = TableOperation.insertOrReplace(pe);
        TableResult ins = tbl.execute(op);
        LOG.debug("Insert: {}", ins.getHttpStatusCode());
      } catch (StorageException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close() throws IOException {
      // empty
    }

  }

  static class RegionEntry extends TableServiceEntity {
    private String partition;
    private FileRegion alias;
    public RegionEntry() {
    }
    RegionEntry(FileRegion alias, String partition) {
      this.alias = alias;
      this.partition = partition;
    }
    public FileRegion getAlias() {
      return alias;
    }
    @Override
    public String getPartitionKey() {
      return partition;
      //return alias.getPath().toUri().getAuthority();
    }
    @Override
    public String getRowKey() {
      long blockId = alias.getBlock().getBlockId();
      return String.valueOf(blockId);
    }
    @Override
    public HashMap<String,EntityProperty> writeEntity(OperationContext opCtxt)
        throws StorageException {
      Block blk = alias.getBlock();
      HashMap<String,EntityProperty> ret = new HashMap<>();
      ret.put("path",     new EntityProperty(alias.getPath().toString()));
      ret.put("offset",   new EntityProperty(alias.getOffset()));
      ret.put("length",   new EntityProperty(blk.getNumBytes()));
      ret.put("blockId",  new EntityProperty(blk.getBlockId()));
      //ret.put("gen", new EntityProperty(blk.getGenerationStamp()));
      return ret;
    }
    @Override
    public void readEntity(HashMap<String,EntityProperty> p,
        OperationContext opContext) throws StorageException {
      alias = new FileRegion(
          p.get("blockId").getValueAsLong(),
          new Path(p.get("path").getValueAsString()),
          p.get("offset").getValueAsLong(),
          p.get("length").getValueAsLong());
    }
    @Override
    public String toString() {
      return alias.toString();
    }
  }

}
