package io.bespin.scala.spark.wordcount;

import io.bespin.scala.util.Tokenizer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

object WordCount extends Tokenizer {
  val usage = """
    Usage: spark-submit --class io.bespin.scala.spark.wordcount.WordCount target/bespin.jar [input] [output]
  """

  def main(args: Array[String]) {
    if (args.length == 0) {
      println(usage)
      System.exit(-1);
    }

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val counts = textFile
      .flatMap(line => tokenize(line))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
  }
}
