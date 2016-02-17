package io.bespin.scala.mapreduce.wordcount

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.rogach.scallop._

import scala.collection.JavaConverters._
import scala.collection.mutable

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object WordCount extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  object MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      tokenize(value).foreach(word => context.write(word, 1))
    }
  }

  object MyMapperIMC extends Mapper[LongWritable, Text, Text, IntWritable] {
    val counts = new mutable.HashMap[String, Int]().withDefaultValue(0)

    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      tokenize(value).foreach(word => counts.put(word, counts(word) + 1))
    }

    override def cleanup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      counts.foreach({ case (k, v) => context.write(k, v) })
    }
  }

  object MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
      val sum = values.asScala.foldLeft(0)(_ + _)
      context.write(key, sum)
    }
  }

  override def run(argv: Array[String]) : Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Use in-mapper combining: " + args.imc())

    val thisJob =
      job("WordCount", getConf)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(if(args.imc()) MyMapperIMC else MyMapper)
        .combine(MyReducer)
        .reduce(MyReducer, args.reducers())

    time {
        thisJob.saveAsTextFile(new Path(args.output()))
    }

    0
  }
}
