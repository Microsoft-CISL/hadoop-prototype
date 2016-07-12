/**
 * 
 */
package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DATA_DIR_KEY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.ReplicaState;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.LengthInputStream;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.FsDatasetUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProvidedReplica extends ReplicaInfo {

  public static final Logger LOG = LoggerFactory.getLogger(ProvidedReplica.class);

  private URI fileURI;
  private long fileOffset;
  private Configuration conf;
  FileSystem remoteFS;
  
  //TODO metaFile can also be remote?
  private final File metaFile;
  
  /**
   * Constructor
   * @param blockId block id
   * @param fileURI remote URI this block is to be read from
   * @param fileOffset the offset in the remote URI
   * @param blockLen the length of the block
   * @param genStamp the generation stamp of the block
   * @param volume the volume this block belongs to
   */
  public ProvidedReplica(long blockId, URI fileURI, long fileOffset, long blockLen, 
      long genStamp, FsVolumeSpi volume, Configuration conf) {
    super(blockId, blockLen, genStamp, volume, null);
    this.fileURI = fileURI;
    this.fileOffset = fileOffset;
    this.conf = conf;
    try {
      this.remoteFS = FileSystem.get(fileURI, this.conf);
    } catch (IOException e) {
      LOG.warn("Failed to obtain filesystem for " + fileURI);
      this.remoteFS = null;
    }
    this.metaFile = createMetaFile();
  }

  private File createMetaFile() {
    //TODO create a meta file that has checksums for actual data in the Provided block
    Collection<String> dnDirectories =
        conf.getTrimmedStringCollection(DFS_DATANODE_DATA_DIR_KEY);
    File dir = new File(".");
    if (dnDirectories.isEmpty()) {
      LOG.warn("No configured data directories");
    }
    for (String d : dnDirectories) {
      try {
        // Strip off [PROVIDED] prefix.
        StorageLocation storageLocation = StorageLocation.parse(d);
        dir = storageLocation.getFile();
        break;
      } catch (IOException e) {
        LOG.warn("Unable to parse storage location for data directory: " + d);
      }
    }

    return FsDatasetUtil.createNullChecksumFile(new File(dir,
        DatanodeUtil.getMetaName(getBlockName(), getGenerationStamp())));
  }

  public ProvidedReplica (ProvidedReplica r) {
    super(r);
    this.fileURI = r.fileURI;
    this.fileOffset = r.fileOffset;
    this.conf = r.conf;
    
    try {
      this.remoteFS = FileSystem.newInstance(fileURI, this.conf);
    } catch (IOException e) {
      this.remoteFS = null;
    }
    
    this.metaFile = createMetaFile();
  }
  
  @Override
  public ReplicaState getState() {
    return ReplicaState.PROVIDED;
  }

  @Override
  public long getBytesOnDisk() {
    return getNumBytes();
  }

  @Override
  public long getVisibleLength() {
    return getNumBytes(); //all bytes are visible
  }
  
  @Override
  public URI getBlockURI() {
    return this.fileURI; 
  }

  @Override
  public String getDatastoreName() {
    //TODO make sure this is correct/consistent with the rest of the names
    return getBlockName();
  }

  @Override
  public InputStream getDataInputStream(long seekOffset) throws IOException {
    
    if (remoteFS != null) {
      FSDataInputStream ins = remoteFS.open(new Path(fileURI));
      ins.seek(fileOffset + seekOffset);
      return new FSDataInputStream(ins);
    }
    else
      throw new IOException("Remote filesystem for provided replica " + this + " does not exist");
  }
  
  @Override
  public OutputStream getDataOutputStream(boolean append) throws IOException {
    //TODO may be append?
    throw new IOException("OutputDataStream is not implemented");
  }
  
  @Override
  public boolean dataSourceExists() {
    if(remoteFS != null) {
      try {
        return remoteFS.exists(new Path(fileURI));
      } catch (IOException e) {
        return false;
      }
    }
    else {
      return false;
    }
  }
  
  @Override
  public boolean deleteDataSource() {
    //TODO
    return false;
  }
  
  @Override
  public long getDataSourceLength() {
    return this.getNumBytes();
  }
  
  @Override
  public File getMetaFile() {
    //TODO have to deprecate this API
    return metaFile;
  }
  
  @Override
  public URI getMetadataURI() {
    return metaFile.toURI();
  }
    
  @Override
  public LengthInputStream getMetadataInputStream() throws IOException {
    return new LengthInputStream(new FileInputStream(metaFile), metaFile.length());
  }
  
  @Override
  public OutputStream getMetadataOutputStream(boolean append) throws IOException {
    return new FileOutputStream(metaFile, append);
  }
  
  @Override
  public boolean metadataSourceExists() {
    return metaFile.exists();
  }
  
  @Override
  public boolean deleteMetadataSource() {
    //TODO 
    return false;
  }
  
  @Override
  public long getMetadataSourceLength() {
    return metaFile.length();
  }
  
}
