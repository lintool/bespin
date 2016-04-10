package io.bespin.scala.mapreduce.bfs

import io.bespin.java.mapreduce.bfs.BfsNode
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper}
import org.apache.hadoop.io.IntWritable
import org.rogach.scallop.ScallopConf

/**
  * Tool for extracting nodes that are reachable from the source node.
  */
object FindReachableNodes extends BaseConfiguredTool with MapReduceSugar {

  // Very simple mapper which essentially just filters out all nodes which have
  // "infinite" distance
  private object MyMapper extends TypedMapper[IntWritable, BfsNode, IntWritable, BfsNode] {
    override def map(nid: IntWritable, node: BfsNode, context: Context): Unit = {
      if(node.getDistance < Int.MaxValue) {
        context.write(nid, node)
      }
    }
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)

    mainOptions = Seq(input, output)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Tool name: " + this.getClass.getName)
    log.info(" - inputDir: " + args.input())
    log.info(" - outputDir: " + args.output())

    val conf = getConf
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024)

    val thisJob = job(s"FindReachableNodes:[input: ${args.input()}, " +
      s"output: ${args.output()}]", conf)
      .sequenceFile[IntWritable, BfsNode](args.input())
      .map(MyMapper)
      .reduce(0)

    thisJob.saveAsTextFile(args.output(), deleteExisting = true)

    0
  }
}
