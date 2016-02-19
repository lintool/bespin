package io.bespin.scala.mapreduce.bigram

import java.lang.Iterable

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Partitioner
import tl.lin.data.pair.PairOfStrings

import scala.collection.JavaConversions._

object ComputeBigramRelativeFrequencyPairs extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  private val wildcard: String = "*"
  private val one: FloatWritable = 1.0f

  private object PairsMapper extends TypedMapper[LongWritable, Text, PairOfStrings, FloatWritable] {

    override def map(key: LongWritable, value: Text, context: Context): Unit = {
      val tokens = tokenize(value)
      if(tokens.length >= 2) {
        tokens.iterator.zip(tokens.tail.iterator).foreach {
          case (left, right) =>
            context.write((left, right), one)
            context.write((left, wildcard), one)
        }}

    }
  }

  /**
    * Combiner object for pairs. Simply sums up the count of all (pair, count) keys sharing the same pair.
    */
  private object PairsCombiner extends TypedReducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable] {
    override def reduce(key: PairOfStrings, values: Iterable[FloatWritable], context: Context): Unit = {
      context.write(key, values.foldLeft(0.0f)(_ + _))
    }
  }

  private object PairsReducer extends TypedReducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable] {
    private var marginal: Float = 0.0f

    override def reduce(key: PairOfStrings, values: Iterable[FloatWritable], context: Context): Unit = {

      val sum = values.foldLeft(0.0f)(_ + _)

      if(key.getRightElement == wildcard) {
        context.write(key, sum)
        marginal = sum
      } else {
        context.write(key, sum / marginal)
      }
    }
  }

  private object PairsPartitioner extends Partitioner[PairOfStrings, FloatWritable] {
    override def getPartition(key: PairOfStrings, value: FloatWritable, numPartitions: Int): Int =
      (key.getLeftElement.hashCode & Int.MaxValue) % numPartitions
  }

  override def run(argv: Array[String]): Int = {
    val args = new ConfWithOutput(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text output: " + args.textOutput())

    val thisJob =
      job("Bigram Relative Frequency - Pairs", getConf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(PairsMapper)
        .combine(PairsCombiner)
        .partition(PairsPartitioner)
        .reduce(PairsReducer, args.reducers())

    time {
      if (args.textOutput())
        thisJob.saveAsTextFile(new Path(args.output()))
      else
        thisJob.saveAsSequenceFile(new Path(args.output()))
    }

    0
  }

}
