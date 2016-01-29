package io.bespin.scala.mapreduce.bigram

import java.lang.Iterable

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{FloatWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Partitioner, Reducer}
import org.apache.log4j.Logger
import tl.lin.data.pair.PairOfStrings

import scala.collection.JavaConversions._

object ComputeBigramRelativeFrequencyPairs extends BaseConfiguredTool with Tokenizer with MapReduceSugar {
  private val log = Logger.getLogger(getClass.getName)

  private val wildcard: String = "*"
  private val one: FloatWritable = 1.0f

  private object PairsMapper extends Mapper[LongWritable, Text, PairOfStrings, FloatWritable] {
    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, PairOfStrings, FloatWritable]#Context): Unit = {
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
  private object PairsCombiner extends Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable] {
    override def reduce(key: PairOfStrings,
                        values: Iterable[FloatWritable],
                        context: Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable]#Context): Unit = {
      var sum: Float = 0.0f
      values.foreach(v => sum += v)
      context.write(key, sum)
    }
  }

  private object PairsReducer extends Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable] {
    private var marginal: Float = 0.0f

    override def reduce(key: PairOfStrings,
                        values: Iterable[FloatWritable],
                        context: Reducer[PairOfStrings, FloatWritable, PairOfStrings, FloatWritable]#Context): Unit = {
      var sum: Float = 0.0f
      values.foreach(v => sum += v)

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

    val conf = getConf

    val thisJob =
      job("Bigram Relative Frequency - Pairs", conf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(PairsMapper)
        .combine(PairsCombiner)
        .partitionBy(PairsPartitioner)
        .reduce(PairsReducer)
        // Set the output path, deleting anything already there
        .setOutputAsFile(new Path(args.output()), deleteExisting = true)

    val jobWithOutput = if(args.textOutput())
      thisJob.withFormat[TextOutputFormat[PairOfStrings, FloatWritable]]
    else
      thisJob.withFormat[SequenceFileOutputFormat[PairOfStrings, FloatWritable]]

    val startTime = System.currentTimeMillis()
    jobWithOutput.run(verbose = true)
    log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

    0
  }

}
