package io.bespin.scala.mapreduce.wordcount;

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

import java.util.StringTokenizer

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import scala.collection.mutable._

object WordCountIMC extends Configured with Tool with WritableConversions with Tokenizer {
  val usage = """
    Usage: hadoop jar target/bespin.jar io.bespin.scala.mapreduce.wordcount.WordCountIMC [input] [output]
  """

  class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    val counts = new HashMap[String, Int]()  { override def default(key: String) = 0 }

    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      tokenize(value.toString).foreach(word => counts.put(word, counts(word) + 1))
    }

    override def cleanup(context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      counts.foreach({ case (k, v) => context.write(k, v) })
    }
  }

  class MyReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
      context.write(key, values.reduceLeft((a, b) => a + b));
    }
  }

  override def run(args: Array[String]) : Int = {
    val conf = getConf();
    val job = Job.getInstance(conf);

    val mapper = new MyMapper

    println("Input: " + args(0))
    println("Output: " + args(1))

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])

    job.waitForCompletion(true);

    -1
  }

  def main(args: Array[String]) {
    ToolRunner.run(this, args)
  }
}
