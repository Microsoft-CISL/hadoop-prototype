package org.apache.hadoop.hdfs.server.namenode;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.BlockFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileSystemImage implements Tool {

  Configuration conf;

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    // require absolute URI to write anywhere but local
    FileSystem.setDefaultUri(conf, new File(".").toURI().toString());
  }

  protected void printUsage() {
  HelpFormatter formatter = new HelpFormatter();
  formatter.printHelp("fs2img [OPTIONS] URI", new Options());
  formatter.setSyntaxPrefix("");
  formatter.printHelp("Options", options());
    ToolRunner.printGenericCommandUsage(System.out);
  }

  static Options options() {
    Options options = new Options();
    options.addOption("o", "outdir", true, "Output directory");
    options.addOption("u", "ugiclass", true, "UGI resolver class");
    options.addOption("b", "blockclass", true, "Block output class");
    options.addOption("i", "blockidclass", true, "Block resolver class");
    options.addOption("c", "cachedirs", true, "Max active dirents");
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

    ImageWriter.Options opts =
      ReflectionUtils.newInstance(ImageWriter.Options.class, getConf());
    for (Option o : cmd.getOptions()) {
      switch (o.getOpt()) {
        case "o":
          opts.output(o.getValue());
          break;
        case "u":
          opts.ugi(Class.forName(o.getValue()).asSubclass(UGIResolver.class));
          break;
        case "b":
          opts.blocks(
              Class.forName(o.getValue()).asSubclass(BlockFormat.class));
          break;
        case "i":
          opts.blockIds(
              Class.forName(o.getValue()).asSubclass(BlockResolver.class));
          break;
        case "c":
          opts.cache(Integer.parseInt(o.getValue()));
          break;
        default:
          throw new UnsupportedOperationException("Internal error");
      }
    }

    String[] rem = cmd.args();
    if (remaining.length != 1) {
      printUsage();
      return -1;
    }

    try (ImageWriter w = new ImageWriter(opts)) {
      for (TreePath e : new FSTreeWalk(new Path(argv[rem]), getConf())) {
        w.accept(e); // add and continue
      }
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new FileSystemImage(), argv);
    System.exit(ret);
  }

}
