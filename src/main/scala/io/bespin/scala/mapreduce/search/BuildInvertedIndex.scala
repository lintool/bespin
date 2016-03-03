package io.bespin.scala.mapreduce.search

import java.util.Collections

import io.bespin.scala.mapreduce.util.{TypedReducer, TypedMapper, MapReduceSugar, BaseConfiguredTool}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, LongWritable}
import org.rogach.scallop.ScallopConf
import tl.lin.data.array.ArrayListWritable
import tl.lin.data.fd.{Object2IntFrequencyDistributionEntry, Object2IntFrequencyDistribution}
import tl.lin.data.pair.{PairOfWritables, PairOfInts}

import scala.collection.JavaConverters._

object BuildInvertedIndex extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  private type IndexRow = PairOfWritables[IntWritable, ArrayListWritable[PairOfInts]]

  private object MyMapper extends TypedMapper[LongWritable, Text, Text, PairOfInts] {
    private val counts: Object2IntFrequencyDistribution[String] =
      new Object2IntFrequencyDistributionEntry[String]()

    override def map(docno: LongWritable, value: Text, context: Context): Unit = {
      val tokens = tokenize(value)

      // Build a histogram of the terms
      counts.clear()
      tokens.foreach( counts.increment )

      // Emit postings
      counts.iterator().asScala.foreach { e =>
        context.write(e.getLeftElement, (docno.get().toInt, e.getRightElement))
      }
    }
  }

  private object MyReducer extends TypedReducer[Text, PairOfInts, Text, IndexRow] {
    override def reduce(key: Text, values: Iterable[PairOfInts], context: Context): Unit = {
      val iter = values.iterator

      val postings = new ArrayListWritable[PairOfInts]()
      while(iter.hasNext) {
        postings.add(iter.next().clone())
      }

      Collections.sort(postings)

      val docFreq: IntWritable = postings.size
      context.write(key, (docFreq, postings))
    }
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)

    mainOptions = Seq(input, output)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())

    val thisJob =
      job("Inverted Index Construction", getConf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(MyMapper)
        .reduce(MyReducer)

    time {
      thisJob.saveAsMapFile(new Path(args.output()))
    }

    0
  }
}
