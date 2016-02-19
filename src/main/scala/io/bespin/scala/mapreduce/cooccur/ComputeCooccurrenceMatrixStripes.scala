package io.bespin.scala.mapreduce.cooccur

import java.lang.Iterable

import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import tl.lin.data.map.HMapStIW

import scala.collection.JavaConverters._

object ComputeCooccurrenceMatrixStripes extends BaseConfiguredTool with Tokenizer with MapReduceSugar {

  private object MyMapper extends TypedMapper[LongWritable, Text, Text, HMapStIW] {
    private var windowSize: Int = _
    private val map: HMapStIW = new HMapStIW

    override def setup(context: Context): Unit = {
      windowSize = context.getConfiguration.getInt("window", 2)
    }

    override def map(key: LongWritable, value: Text, context: Context): Unit = {
      val tokens = tokenize(value).toArray

      var i, j = 0
      while(i < tokens.length) {
        j = Math.max(i - windowSize, 0)
        map.clear()
        while(j < Math.min(i + windowSize + 1, tokens.length)) {
          if(i != j)
            map.increment(tokens(j))
          j += 1
        }
        context.write(tokens(i), map)
        i += 1
      }
    }

  }

  private object MyReducer extends TypedReducer[Text, HMapStIW, Text, HMapStIW] {

    override def reduce(key: Text, values: Iterable[HMapStIW], context: Context): Unit = {
      val iter = values.iterator().asScala
      val map = new HMapStIW
      iter.foreach { map.plus }
      context.write(key, map)
    }

  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Window: " + args.window())
    log.info("Number of reducers: " + args.reducers())

    val config = getConf
    config.setInt("window", args.window())

    val thisJob =
      job("Bigram Relative Frequency - Pairs", config)
        // Set the input path of the source text file
        .textFile(new Path(args.input()))
        // Map and reduce over the data of the source file
        .map(MyMapper)
        .combine(MyReducer)
        .reduce(MyReducer, args.reducers())

    time {
      thisJob.saveAsTextFile(new Path(args.output()))
    }

    0
  }

}
