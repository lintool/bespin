package io.bespin.java.mapreduce.bigram;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.map.HMapStFW;
import tl.lin.data.map.MapKF;

public class ComputeBigramRelativeFrequencyStripes  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ComputeBigramRelativeFrequencyStripes.class);

  protected static class MyMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
    private static final HMapStFW MAP = new HMapStFW();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();

      List<String> tokens = new ArrayList<String>();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        tokens.add(w);
      }

      if (tokens.size() < 2) return;
      for (int i = 1; i < tokens.size(); i++) {
        MAP.clear();
        MAP.put(tokens.get(i), 1);
        KEY.set(tokens.get(i - 1));
        context.write(KEY, MAP);
      }
    }
  }

  private static class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static class MyReducer extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      float sum = 0.0f;
      for (MapKF.Entry<String> entry : map.entrySet()) {
        sum += entry.getValue();
      }
      for (String term : map.keySet()) {
        map.put(term, map.get(term) / sum);
      }

      context.write(key, map);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private ComputeBigramRelativeFrequencyStripes() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;

    @Option(name = "-textOutput", required = false, usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    public boolean textOutput = false;
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

    LOG.info("Tool name: " + ComputeBigramRelativeFrequencyStripes.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);

    Job job = Job.getInstance(getConf());
    job.setJobName(ComputeBigramRelativeFrequencyStripes.class.getSimpleName());
    job.setJarByClass(ComputeBigramRelativeFrequencyStripes.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStFW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HMapStFW.class);
    if (args.textOutput) {
      job.setOutputFormatClass(TextOutputFormat.class);
    } else {
      job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
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
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ComputeBigramRelativeFrequencyStripes(), args);
  }
}
