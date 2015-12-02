package io.bespin.demo.smr.wordcount;

import io.bespin.demo.Tokenizer
import io.bespin.demo.WritableConversions

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

object WordCount extends Configured with Tool with WritableConversions with Tokenizer {
  val usage = """
    Usage: hadoop jar target/bespin.jar io.bespin.demo.smr.wordcount.WordCount [input] [output]
  """

  class MyMapper extends Mapper[LongWritable, Text, Text, IntWritable] {
    override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, IntWritable]#Context) = {
      tokenize(value.toString).foreach(word => context.write(word, 1))
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
