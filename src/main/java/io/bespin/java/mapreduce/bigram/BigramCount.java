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

package io.bespin.java.mapreduce.bigram;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Counts bigrams.
 */
public class BigramCount extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BigramCount.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text BIGRAM = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      if (tokens.size() < 2) return;
      for (int i = 1; i < tokens.size(); i++) {
        BIGRAM.set(tokens.get(i - 1) + " " + tokens.get(i));
        context.write(BIGRAM, ONE);
      }
    }
  }

  private static final class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private BigramCount() {}

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

    LOG.info("Tool name: " + BigramCount.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);

    Job job = Job.getInstance(getConf());
    job.setJobName(BigramCount.class.getSimpleName());
    job.setJarByClass(BigramCount.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BigramCount(), args);
  }
}
