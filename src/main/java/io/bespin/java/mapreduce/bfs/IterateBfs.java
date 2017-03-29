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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListOfInts;
import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapII;
import tl.lin.data.map.MapII;

import java.io.IOException;
import java.util.Iterator;

/**
 * Tool for running one iteration of parallel breadth-first search.
 *
 * @author Jimmy Lin
 */
public class IterateBfs extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(IterateBfs.class);

  private enum ReachableNodes {
    ReachableInMapper, ReachableInReducer
  }

  // Mapper with in-mapper combiner optimization.
  private static final class MapClass extends Mapper<IntWritable, BfsNode, IntWritable, BfsNode> {
    // For buffering distances keyed by destination node.
    private static final HMapII map = new HMapII();

    // For passing along node structure.
    private static final BfsNode intermediateStructure = new BfsNode();

    @Override
    public void map(IntWritable nid, BfsNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(BfsNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      if (node.getDistance() == Integer.MAX_VALUE) {
        return;
      }

      context.getCounter(ReachableNodes.ReachableInMapper).increment(1);
      // Retain distance to self.
      map.put(nid.get(), node.getDistance());

      ArrayListOfInts adj = node.getAdjacenyList();
      int dist = node.getDistance() + 1;
      // Keep track of shortest distance to neighbors.
      for (int i = 0; i < adj.size(); i++) {
        int neighbor = adj.get(i);

        // Keep track of distance if it's shorter than previously
        // encountered, or if we haven't encountered this node.
        if ((map.containsKey(neighbor) && dist < map.get(neighbor)) || !map.containsKey(neighbor)) {
          map.put(neighbor, dist);
        }
      }
    }

    @Override
    public void cleanup(Mapper<IntWritable, BfsNode, IntWritable, BfsNode>.Context context)
        throws IOException, InterruptedException {
      // Now emit the messages all at once.
      IntWritable k = new IntWritable();
      BfsNode dist = new BfsNode();

      for (MapII.Entry e : map.entrySet()) {
        k.set(e.getKey());

        dist.setNodeId(e.getKey());
        dist.setType(BfsNode.Type.Distance);
        dist.setDistance(e.getValue());

        context.write(k, dist);
      }
    }
  }

  // Reduce: select smallest of incoming distances, rewrite graph structure.
  private static final class ReduceClass extends Reducer<IntWritable, BfsNode, IntWritable, BfsNode> {
    private static final BfsNode node = new BfsNode();

    @Override
    public void reduce(IntWritable nid, Iterable<BfsNode> iterable, Context context)
        throws IOException, InterruptedException {

      Iterator<BfsNode> values = iterable.iterator();

      int structureReceived = 0;
      int dist = Integer.MAX_VALUE;
      while (values.hasNext()) {
        BfsNode n = values.next();

        if (n.getType() == BfsNode.Type.Structure) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;

          int arr[] = new int[list.size()];
          for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i);
          }

          node.setAdjacencyList(new ArrayListOfIntsWritable(arr));
        } else {
          // This is a message that contains distance.
          if (n.getDistance() < dist) {
            dist = n.getDistance();
          }
        }
      }

      node.setType(BfsNode.Type.Complete);
      node.setNodeId(nid.get());
      node.setDistance(dist); // Update the final distance.

      if (dist != Integer.MAX_VALUE) {
        context.getCounter(ReachableNodes.ReachableInReducer).increment(1);
      }

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated
        // distance.
        context.write(nid, node);
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing
        // to a node which has no corresponding node structure (i.e.,
        // distance was passed to a non-existent node)... log but move
        // on.
        LOG.warn("No structure received for nodeid: " + nid.get());
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " struct: " + structureReceived);
      }
    }
  }

  private IterateBfs() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-partitions", metaVar = "[num]", required = true, usage = "number of partitions")
    int partitions;
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
    LOG.info(" - input: " + args.input);
    LOG.info(" - output: " + args.output);
    LOG.info(" - partitions: " + args.partitions);

    getConf().set("mapred.child.java.opts", "-Xmx2048m");

    Job job = Job.getInstance(getConf());
    job.setJobName(String.format("IterateBfs[input: %s, output: %s, partitions: %d]",
        args.input, args.output, args.partitions));
    job.setJarByClass(EncodeBfsGraph.class);

    job.setNumReduceTasks(args.partitions);

    FileInputFormat.addInputPath(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(BfsNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(BfsNode.class);

    job.setMapperClass(MapClass.class);
    job.setReducerClass(ReduceClass.class);

    // Delete the output directory if it exists already.
    FileSystem.get(job.getConfiguration()).delete(new Path(args.output), true);

    job.waitForCompletion(true);

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new IterateBfs(), args);
    System.exit(res);
  }
}