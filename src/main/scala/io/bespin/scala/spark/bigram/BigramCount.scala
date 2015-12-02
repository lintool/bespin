package io.bespin.scala.spark.bigram;

import java.util.StringTokenizer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

object BigramCount {
  val usage = """
    Usage: spark-submit --class io.bespin.scala.spark.bigram.BigramCount target/bespin.jar [input] [output]
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
      .flatMap(line => {
        val tokens = new StringTokenizer(line).toList
          .map(_.asInstanceOf[String].toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
          .filter(_.length != 0)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
  }
}
