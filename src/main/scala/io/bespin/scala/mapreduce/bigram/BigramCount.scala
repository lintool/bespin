package io.bespin.scala.mapreduce.bigram

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

import scala.collection.JavaConversions._

object BigramCount extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  private object BigramMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      val tokens = tokenize(value)
      if (tokens.length > 1)
        tokens.iterator.zip(tokens.tail.iterator)
          .map { case (left, right) => s"$left $right" }
          .foreach(word => context.write(word, 1))
    }
  }

  private object BigramReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
      context.write(key, values.foldLeft(0)(_ + _))
    }
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val thisJob =
      job("Word Count", getConf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(BigramMapper)
        .combine(BigramReducer)
        .reduce(BigramReducer, args.reducers())

    time {
      thisJob.saveAsTextFile(new Path(args.output()))
    }

    0
  }

}
