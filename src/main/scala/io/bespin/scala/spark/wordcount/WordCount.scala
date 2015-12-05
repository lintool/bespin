package io.bespin.scala.spark.wordcount;

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
}

object WordCount extends Tokenizer {
  val log = Logger.getLogger(getClass().getName());

  def wcIter(iter: Iterator[String]): Iterator[(String, Int)] = {
    val counts = new HashMap[String, Int]() { override def default(key: String) = 0 }

    iter.flatMap(line => tokenize(line))
      .foreach { t => counts.put(t, counts(t) + 1) }

    counts.iterator
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Use in-mapper combining: " + args.imc())

    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output());
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true);

    val textFile = sc.textFile(args.input())

    if (!args.imc()) {
      textFile
        .flatMap(line => tokenize(line))
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .saveAsTextFile(args.output())
    } else {
      textFile
        .mapPartitions(wcIter)
        .reduceByKey(_ + _)
        .saveAsTextFile(args.output())
    }
  }
}
