package io.bespin.scala.mapreduce.bigram


import io.bespin.scala.util.{BaseConfiguredRunnable, Tokenizer}
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.log4j._

import scala.collection.JavaConversions._

object BigramCount extends BaseConfiguredRunnable with Tokenizer with MapReduceSugar {
  private val log = Logger.getLogger(getClass.getName)

  private object BigramMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      val tokens = tokenize(value)
      if (tokens.length > 1)
        tokens.sliding(2).map(p => p.mkString(" ")).foreach(word => context.write(word, 1))
    }
  }


  private object BigramReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
      // Although it is possible to write the reducer in a functional style (e.g., with foldLeft),
      // an imperative implementation is clearer for two reasons:
      //
      //   (1) The MapReduce framework supplies an iterable over writable objects; since writable
      //       are container objects, it simply returns (a reference to) the same object each time
      //       but with a different payload inside it.
      //   (2) Implicit writable conversions in WritableConversions.
      //
      // The combination of both means that a functional implementation may have unpredictable
      // behavior when the two issues interact.
      var sum: Int = 0
      values.foreach(v => sum += v)
      context.write(key, sum)
    }
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = getConf

    val thisJob =
      job("Word Count", conf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(BigramMapper)
        .combine(BigramReducer)
        .reduce(BigramReducer, args.reducers())
        // Set the output path, deleting anything already there, and output in Text format
        .setOutputAsFile(new Path(args.output()), deleteExisting = true)
        .withFormat[TextOutputFormat[Text, IntWritable]]

    val startTime = System.currentTimeMillis()
    thisJob.run(verbose = true)
    log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

    0
  }

}
