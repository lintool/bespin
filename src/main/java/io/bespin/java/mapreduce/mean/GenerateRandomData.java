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

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
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
import java.util.Random;

/**
 * Program that generates random data.
 */
public class GenerateRandomData extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(GenerateRandomData.class);

  private static final class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final Random random = new Random(System.nanoTime());
    private int keyLength = 1;
    private int valueRange = Integer.MAX_VALUE;

    @Override
    public void setup(Context context) {
      keyLength = context.getConfiguration().getInt("keyLength", 1);
      valueRange = context.getConfiguration().getInt("valueRange", Integer.MAX_VALUE);
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      int n = Integer.parseInt(value.toString());
      for (int i = 0; i < n; i++) {
        context.write(new Text(RandomStringUtils.randomAlphabetic(keyLength)),
            new IntWritable(random.nextInt(valueRange)));
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private GenerateRandomData() {}

  private static final class Args {
    @Option(name = "-mappers", metaVar = "[num]", usage = "number of mappers")
    int mappers = 10;

    @Option(name = "-num", metaVar = "[num]", usage = "number of random numbers per mapper")
    int num = 100;

    @Option(name = "-keyLength", metaVar = "[num]", usage = "length of each key")
    int keyLength = 1;

    @Option(name = "-valueRange", metaVar = "[num]", usage = "range of each value")
    int valueRange = Integer.MAX_VALUE;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;
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

    LOG.info("Tool: " + GenerateRandomData.class.getSimpleName());
    LOG.info(" - number of mappers: " + args.mappers);
    LOG.info(" - random numbers per mapper: " + args.num);
    LOG.info(" - key length: " + args.keyLength);
    LOG.info(" - value range: " + args.valueRange);
    LOG.info(" - output path: " + args.output);

    Configuration conf = getConf();
    Random rand = new Random();

    Path tmp = new Path("tmp" + rand.nextInt(1000) + ".txt");
    FileSystem fs = FileSystem.get(conf);
    FSDataOutputStream dataOut = fs.create(tmp);
    for (int i=0; i<args.mappers; i++) {
      dataOut.write((args.num + "\n").getBytes());
    }
    dataOut.close();

    Job job = Job.getInstance(conf);
    job.setJobName(GenerateRandomData.class.getSimpleName());
    job.setJarByClass(GenerateRandomData.class);

    job.getConfiguration().setInt("keyLength", args.keyLength);
    job.getConfiguration().setInt("valueRange", args.valueRange);

    job.setNumReduceTasks(0);

    job.setInputFormatClass(NLineInputFormat.class);
    job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);
    NLineInputFormat.addInputPath(job, tmp);

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    fs.delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    fs.delete(tmp, true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new GenerateRandomData(), args);
  }
}
