package io.bespin.scala.mapreduce.cooccur

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Partitioner
import tl.lin.data.pair.PairOfStrings

import java.lang.Iterable

import scala.collection.JavaConverters._

/**
  * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
  * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with
  * MapReduce" by Lin &amp; Dyer, as well as the following paper:
  *
  * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
  * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
  * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
  *
  * @see [[io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixPairs]]
  */
object ComputeCooccurrenceMatrixPairs extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  private object MyMapper extends TypedMapper[LongWritable, Text, PairOfStrings, IntWritable] {
    private var windowSize: Int = _

    override def setup(context: Context): Unit = {
      windowSize = context.getConfiguration.getInt("window", 2)
    }

    override def map(key: LongWritable, value: Text, context: Context): Unit = {
      val tokens = tokenize(value).toArray
      tokens.zipWithIndex.foreach { case (token, i) =>
        val start = Math.max(i - windowSize, 0)
        val end = Math.min(i + windowSize + 1, tokens.length)
        (start until end).foreach { j =>
          if(i != j)
            context.write((token, tokens(j)), 1)
        }
      }
    }

  }

  private object MyReducer extends TypedReducer[PairOfStrings, IntWritable, PairOfStrings, IntWritable] {

    override def reduce(key: PairOfStrings, values: Iterable[IntWritable], context: Context): Unit = {
      val iter = values.iterator().asScala
      context.write(key, iter.foldLeft(0)(_ + _))
    }

  }

  private object MyPartitioner extends Partitioner[PairOfStrings, IntWritable] {
    override def getPartition(key: PairOfStrings, value: IntWritable, numPartitions: Int): Int =
      (key.getLeftElement.hashCode & Int.MaxValue) % numPartitions
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Window: " + args.window())
    log.info("Number of reducers: " + args.reducers())

    val config = getConf
    config.setInt("window", args.window())

    val thisJob =
      job("Cooccurrence Matrix - Pairs", config)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(MyMapper)
        .combine(MyReducer)
        .partition(MyPartitioner)
        .reduce(MyReducer, args.reducers())

    time {
      thisJob.saveAsTextFile(new Path(args.output()))
    }

    0
  }
}
