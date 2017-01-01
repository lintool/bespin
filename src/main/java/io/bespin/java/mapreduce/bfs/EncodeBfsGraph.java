/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.bespin.java.mapreduce.bfs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.IOException;

/**
 * Tool for taking a plain-text encoding of a directed graph and building corresponding Hadoop
 * structures for running parallel breadth-first search.
 *
 * @author Jimmy Lin
 */
public class EncodeBfsGraph extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(EncodeBfsGraph.class);
  private static final String SRC_KEY = "src";

  private enum Graph {
    Nodes, Edges
  }

  private static final class MyMapper extends Mapper<LongWritable, Text, IntWritable, BfsNode> {
    private static final IntWritable nid = new IntWritable();
    private static final BfsNode node = new BfsNode();
    private static int src;

    @Override
    public void setup(Context context) {
      src = context.getConfiguration().getInt(SRC_KEY, 0);
      node.setType(BfsNode.Type.Complete);
    }

    @Override
    public void map(LongWritable key, Text t, Context context) throws IOException,
        InterruptedException {
      String[] arr = t.toString().trim().split("\\s+");

      int cur = Integer.parseInt(arr[0]);
      nid.set(cur);
      node.setNodeId(cur);
      node.setDistance(cur == src ? 0 : Integer.MAX_VALUE);

      if (arr.length == 1) {
        node.setAdjacencyList(new ArrayListOfIntsWritable());
      } else {
        int[] neighbors = new int[arr.length - 1];
        for (int i = 1; i < arr.length; i++) {
          neighbors[i - 1] = Integer.parseInt(arr[i]);
        }
        node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors));
      }

      context.getCounter(Graph.Nodes).increment(1);
      context.getCounter(Graph.Edges).increment(arr.length - 1);

      context.write(nid, node);
    }
  }

  private EncodeBfsGraph() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-src", metaVar = "[node]", required = true, usage = "source node")
    int src;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - inputDir: " + args.input);
    LOG.info(" - outputDir: " + args.output);
    LOG.info(" - src: " + args.src);

    Job job = Job.getInstance(getConf());
    job.setJobName(String.format("EncodeBfsGraph[input: %s, ouput: %s, src: %d]",
        args.input, args.output, args.src));
    job.setJarByClass(EncodeBfsGraph.class);

    job.setNumReduceTasks(0);

    job.getConfiguration().setInt(SRC_KEY, args.src);
    job.getConfiguration().setInt("mapred.min.split.size", 1024 * 1024 * 1024);

    FileInputFormat.addInputPath(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BfsNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BfsNode.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    FileSystem.get(job.getConfiguration()).delete(new Path(args.output), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new EncodeBfsGraph(), args);
    System.exit(res);
  }
}
