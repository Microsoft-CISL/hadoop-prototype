package org.apache.hadoop.hdfs.server.namenode;

import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hdfs.server.namenode.TreeWalk.TreeIterator;

public class FileSystemImage implements Tool {

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
    options.addOption("h", "help", true, "Print usage");
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
              Class.forName(o.getValue()).asSubclass(BlockOutput.class));
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
 
    try (ImageWriter w = new ImageWriter(opts)) {

      for (TreePath e : new FSTreeWalk(new Path(argv[0]), getConf())) {
        w.accept(e); // add and continue
      }
//      TreeWalk t = new FSTreeWalk(new Path(argv[0]), getConf());
//      for (TreeIterator i = t.iterator(); i.hasNext();) {
//        TreePath p = i.next();
//        if (false) {
//          TreeIterator j = i.fork();
//        }
//      }
//      for (TreeIterator i = t.iterator(); i.hasNext();) {
//        TreePath p = i.next();
//        if (false) {
//          TreeIterator j = i.fork();
//          // i.next() != j.next();
//        }
//      }
    }
    return 0;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new FileSystemImage(), argv);
    System.exit(ret);
  }

}
