package io.bespin.scala.mapreduce.bigram

import java.lang.Iterable

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import tl.lin.data.map.HMapStFW

import scala.collection.JavaConversions._
import scala.collection.mutable

object ComputeBigramRelativeFrequencyStripes extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  private object StripesMapper extends TypedMapper[LongWritable, Text, Text, HMapStFW] {
    override def map(key: LongWritable, value: Text, context: Context): Unit = {
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

  private object StripesCombiner extends TypedReducer[Text, HMapStFW, Text, HMapStFW] {
    override def reduce(key: Text, values: Iterable[HMapStFW], context: Context): Unit = {
      val newMap = new HMapStFW
      values.foreach(v => newMap.plus(v))
      context.write(key, newMap)
    }
  }

  private object StripesReducer extends TypedReducer[Text, HMapStFW, Text, HMapStFW] {
    override def reduce(key: Text, values: Iterable[HMapStFW], context: Context): Unit = {
      val finalMap: HMapStFW = new HMapStFW
      values.foreach(v => finalMap.plus(v))

      val sum = finalMap.values.foldLeft(0.0f)(_ + _)

      finalMap.entrySet().foreach(entry =>
        finalMap.put(entry.getKey, entry.getValue / sum))

      context.write(key, finalMap)
    }
  }

  override def run(argv: Array[String]): Int = {
    val args = new ConfWithOutput(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Text output: " + args.textOutput())

    val thisJob =
      job("Bigram Relative Frequency - Stripes", getConf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(StripesMapper)
        .combine(StripesCombiner)
        .reduce(StripesReducer, args.reducers())

    time {
      if (args.textOutput())
        thisJob.saveAsTextFile(new Path(args.output()))
      else
        thisJob.saveAsSequenceFile(new Path(args.output()))
    }

    0
  }
}
