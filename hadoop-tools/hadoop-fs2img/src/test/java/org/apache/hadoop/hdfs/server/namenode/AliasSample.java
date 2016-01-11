package org.apache.hadoop.hdfs.server.namenode;

import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos.BlockProto;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.hdfs.server.common.TextFileRegionFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AliasSample implements Tool {

  private static final Logger LOG =
    LoggerFactory.getLogger(AliasSample.class);

  Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  protected void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("sample [OPTIONS] URI", new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("Options", options());
      ToolRunner.printGenericCommandUsage(System.out);
  }

  static Options options() {
    Options options = new Options();
    options.addOption("b", "blockclass", true, "Block output class");
    options.addOption("i", "blockidclass", true, "Block resolver class");
    options.addOption("s", "seed", true, "seed");
    options.addOption("h", "help", false, "Print usage");
    return options;
  }

  @Override
  public int run(String[] argv) throws Exception {
    Options options = options();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println(
        "Error parsing command-line options: " + e.getMessage());
      printUsage();
      return -1;
    }

    if (cmd.hasOption("h")) {
      printUsage();
      return -1;
    }

    Random rand = new Random();
    long seed = rand.nextLong();
    @SuppressWarnings("rawtypes")
    Class<? extends BlockFormat> fmtClass = TextFileRegionFormat.class;
    Class<? extends BlockResolver> idClass = FixedBlockResolver.class;
    for (Option o : cmd.getOptions()) {
      switch (o.getOpt()) {
        case "b":
          fmtClass = Class.forName(o.getValue()).asSubclass(BlockFormat.class);
          break;
        case "i":
          idClass = Class.forName(o.getValue()).asSubclass(BlockResolver.class);
          break;
        case "s":
          seed = Long.valueOf(o.getValue());
          break;
        default:
          throw new UnsupportedOperationException("Internal error");
      }
    }
    if (null == fmtClass || null == idClass) {
      printUsage();
      return -1;
    }

    @SuppressWarnings("unchecked")
    BlockFormat<FileRegion> fmt = 
      ReflectionUtils.newInstance(fmtClass, getConf());
    BlockResolver ids =
      ReflectionUtils.newInstance(idClass, getConf());

    LOG.info("seed: {}", seed);
    rand.setSeed(seed);

    long id = 0L;
    try (BlockFormat.Writer<FileRegion> w = fmt.getWriter(null)) {
      for (TreePath e : new RandomTreeWalk(seed)) {
        long off = 0L;
        FileStatus stat = e.getFileStatus();
        e.accept(id++);
        for (BlockProto block : ids.resolve(stat)) {
          FileRegion r = new FileRegion(block.getBlockId(), stat.getPath(),
                off, block.getNumBytes());
          w.store(r);
          off += block.getNumBytes();
        }
      }
    }

    // recreate, discard state, assume deterministic
    ids = ReflectionUtils.newInstance(idClass, getConf());

    id = 0L;
    try (BlockFormat.Reader<FileRegion> r = fmt.getReader(null)) {
      for (TreePath e : new RandomTreeWalk(seed)) {
        long off = 0L;
        FileStatus stat = e.getFileStatus();
        for (BlockProto block : ids.resolve(stat)) {
          FileRegion chk = new FileRegion(block.getBlockId(), stat.getPath(),
                off, block.getNumBytes());
          FileRegion stor = r.resolve(
              new Block(block.getBlockId(), block.getNumBytes(), 1001));
          if (!chk.equals(stor)) {
            LOG.warn("MISMATCH {} {} ", chk, stor);
          }
          off += block.getNumBytes();
        }
        e.accept(id++);
      }
    }
    LOG.info("Done.");

    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new AliasSample(), argv);
    System.exit(ret);
  }

}
