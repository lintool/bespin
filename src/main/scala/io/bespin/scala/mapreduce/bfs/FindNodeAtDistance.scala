package io.bespin.scala.mapreduce.bfs

import io.bespin.java.mapreduce.bfs.BfsNode
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper}
import org.apache.hadoop.io.IntWritable
import org.rogach.scallop.ScallopConf

/**
  * Tool for extracting nodes that are a particular distance from the source node.
  */
object FindNodeAtDistance extends BaseConfiguredTool with MapReduceSugar {

  private val Distance_Key = "distance"

  private object MyMapper extends TypedMapper[IntWritable, BfsNode, IntWritable, BfsNode] {
    private[this] var distance: Int = _

    override def setup(context: Context): Unit = {
      distance = context.getConfiguration.getInt(Distance_Key, 0)
    }

    override def map(nid: IntWritable, node: BfsNode, context: Context): Unit = {
      if(node.getDistance == distance) {
        context.write(nid, node)
      }
    }
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)
    lazy val distance = opt[Int](descr = "distance", required = true)

    mainOptions = Seq(input, output, distance)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Tool name: " + this.getClass.getName)
    log.info(" - inputDir: " + args.input())
    log.info(" - outputDir: " + args.output())
    log.info(" - distance: " + args.distance())

    val conf = getConf
    conf.setInt(Distance_Key, args.distance())
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024)

    val thisJob = job(s"FindNodeAtDistance[input: ${args.input()}, " +
      s"output: ${args.output()}, distance: ${args.distance()}]", conf)
      .sequenceFile[IntWritable, BfsNode](args.input())
      .map(MyMapper)
      .reduce(0)

    thisJob.saveAsTextFile(args.output(), deleteExisting = true)

    0
  }
}
