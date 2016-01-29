package io.bespin.scala.mapreduce.bigram

import java.lang.Iterable

import io.bespin.scala.util.{BaseConfiguredRunnable, Tokenizer}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.log4j.Logger
import tl.lin.data.map.HMapStFW

import scala.collection.JavaConversions._
import scala.collection.mutable

object ComputeBigramRelativeFrequencyStripes extends BaseConfiguredRunnable with Tokenizer with MapReduceSugar {
  private val log = Logger.getLogger(getClass.getSimpleName)

  private object StripesMapper extends Mapper[LongWritable, Text, Text, HMapStFW] {
    override def map(key: LongWritable,
                     value: Text,
                     context: Mapper[LongWritable, Text, Text, HMapStFW]#Context): Unit = {
      val tokens = tokenize(value)
      val stripes: mutable.Map[String, HMapStFW] = mutable.HashMap()
      if(tokens.length >= 2) {
        tokens.iterator.zip(tokens.tail.iterator).foreach {
          case (prev, cur) =>
            val stripe = stripes.getOrElseUpdate(prev, new HMapStFW)
            stripe.increment(cur)
        }
        stripes.foreach { case (k, stripe) => context.write(k, stripe) }
      }
    }
  }

  private object StripesCombiner extends Reducer[Text, HMapStFW, Text, HMapStFW] {
    override def reduce(key: Text,
                        values: Iterable[HMapStFW],
                        context: Reducer[Text, HMapStFW, Text, HMapStFW]#Context): Unit = {
      val newMap = new HMapStFW
      values.foreach(v => newMap.plus(v))
      context.write(key, newMap)
    }
  }

  private object StripesReducer extends Reducer[Text, HMapStFW, Text, HMapStFW] {
    override def reduce(key: Text,
                        values: Iterable[HMapStFW],
                        context: Reducer[Text, HMapStFW, Text, HMapStFW]#Context): Unit = {
      val finalMap: HMapStFW = new HMapStFW
      values.foreach(v => finalMap.plus(v))

      var sum = 0.0f
      finalMap.values.foreach(v => sum += v)

      finalMap.entrySet().foreach(entry => {
        finalMap.put(entry.getKey, entry.getValue / sum)
      })

      context.write(key, finalMap)
    }
  }

  override def run(argv: Array[String]): Int = {
    val args = new ConfWithOutput(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text output: " + args.textOutput())

    val conf = getConf

    val thisJob =
      job("Bigram Relative Frequency - Stripes", conf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(StripesMapper)
        .combine(StripesCombiner)
        .reduce(StripesReducer, args.reducers())
        // Set the output path, deleting anything already there
        .setOutputAsFile(new Path(args.output()), deleteExisting = true)

    val jobWithOutput = if(args.textOutput())
      thisJob.withFormat[TextOutputFormat[Text, HMapStFW]]
    else
      thisJob.withFormat[SequenceFileOutputFormat[Text, HMapStFW]]

    val startTime = System.currentTimeMillis()
    jobWithOutput.run(verbose = true)
    log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

    0
  }
}
