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

package io.bespin.java.mapreduce.mean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfLongs;

import java.io.IOException;
import java.util.Iterator;

/**
 * Program that computes the mean of values associated with each key (version 3).
 * This implementation illustrates proper use of combiners and is faster than version 1.
 */
public class ComputeMeanV3 extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ComputeMeanV3.class);

  private static final class MyMapper extends Mapper<Text, Text, Text, PairOfLongs> {
    @Override
    public void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      context.write(key, new PairOfLongs(Long.parseLong(value.toString()), 1L));
    }
  }

  private static final class MyCombiner extends Reducer<Text, PairOfLongs, Text, PairOfLongs> {
    @Override
    public void reduce(Text key, Iterable<PairOfLongs> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfLongs> iter = values.iterator();
      long sum = 0L;
      long cnt = 0L;
      while (iter.hasNext()) {
        PairOfLongs pair = iter.next();
        sum += pair.getLeftElement();
        cnt += pair.getRightElement();
      }
      context.write(key, new PairOfLongs(sum, cnt));
    }
  }

  private static final class MyReducer extends Reducer<Text, PairOfLongs, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<PairOfLongs> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfLongs> iter = values.iterator();
      long sum = 0L;
      long cnt = 0L;
      while (iter.hasNext()) {
        PairOfLongs pair = iter.next();
        sum += pair.getLeftElement();
        cnt += pair.getRightElement();
      }
      context.write(key, new IntWritable((int) (sum/cnt)));
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private ComputeMeanV3() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;
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

    LOG.info("Tool: " + ComputeMeanV3.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ComputeMeanV3.class.getSimpleName());
    job.setJarByClass(ComputeMeanV3.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.getConfiguration().set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfLongs.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ComputeMeanV3(), args);
  }
}
