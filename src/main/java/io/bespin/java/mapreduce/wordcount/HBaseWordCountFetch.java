package io.bespin.java.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

/**
 * Simple demo of HBase; fetching word counts.
 */
public class HBaseWordCountFetch extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(HBaseWordCountFetch.class);

  /**
   * Creates an instance of this tool.
   */
  public HBaseWordCountFetch() {}

  public static class Args {
    @Option(name = "-term", metaVar = "[term]", required = true, usage = "term to look up")
    public String term;

    @Option(name = "-table", metaVar = "[name]", required = true, usage = "output path")
    public String table;

    @Option(name = "-config", metaVar = "[path]", required = true, usage = "HBase config")
    public String config;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    Configuration conf = getConf();
    conf.addResource(new Path(args.config));

    Configuration hbaseConfig = HBaseConfiguration.create(conf);
    HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
    HTableInterface table = hbaseConnection.getTable(args.table);

    Get get = new Get(Bytes.toBytes(args.term));
    Result result = table.get(get);

    int count = Bytes.toInt(result.getValue(HBaseWordCount.CF, HBaseWordCount.COUNT));

    LOG.info("term: " + args.term + ", count: " + count);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new HBaseWordCountFetch(), args);
  }
}