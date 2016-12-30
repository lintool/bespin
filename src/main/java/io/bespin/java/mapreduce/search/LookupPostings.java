package io.bespin.java.mapreduce.search;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.array.ArrayListWritable;
import tl.lin.data.fd.Int2IntFrequencyDistribution;
import tl.lin.data.fd.Int2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfWritables;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LookupPostings {
  private static final class Args {
    @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
    String index;

    @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
    String collection;

    @Option(name = "-term", metaVar = "[term]", required = true, usage = "term")
    String term;
  }

  public static void main(String[] argv) throws IOException {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(1);
    }

    if (args.collection.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      System.exit(-1);
    }

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    MapFile.Reader reader = new MapFile.Reader(new Path(args.index + "/part-r-00000"), config);

    lookupTerm(args.term, reader, args.collection, fs);

    reader.close();
  }

  public static void lookupTerm(String term, MapFile.Reader reader, String collectionPath,
      FileSystem fs) throws IOException {
    FSDataInputStream collection = fs.open(new Path(collectionPath));

    Text key = new Text();
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
        new PairOfWritables<>();

    key.set(term);
    Writable w = reader.get(key, value);

    if (w == null) {
      System.out.println("\nThe term '" + term + "' does not appear in the collection");
      return;
    }

    ArrayListWritable<PairOfInts> postings = value.getRightElement();
    System.out.println("\nComplete postings list for '" + term + "':");
    System.out.println("df = " + value.getLeftElement());

    Int2IntFrequencyDistribution hist = new Int2IntFrequencyDistributionEntry();
    for (PairOfInts pair : postings) {
      hist.increment(pair.getRightElement());
      System.out.print(pair);
      collection.seek(pair.getLeftElement());
      BufferedReader r = new BufferedReader(new InputStreamReader(collection));

      String d = r.readLine();
      d = d.length() > 80 ? d.substring(0, 80) + "..." : d;

      System.out.println(": " + d);
    }

    System.out.println("\nHistogram of tf values for '" + term + "'");
    for (PairOfInts pair : hist) {
      System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
    }

    collection.close();
  }
}
