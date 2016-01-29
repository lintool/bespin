package io.bespin.scala.mapreduce.bfs

import io.bespin.java.mapreduce.bfs.BfsNode
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.rogach.scallop.ScallopConf
import tl.lin.data.array.ArrayListOfIntsWritable

/**
  * Tool for taking a plain-text encoding of a directed graph and building corresponding Hadoop
  * structures for running parallel breadth-first search.
  */
object EncodeBfsGraph extends BaseConfiguredTool with MapReduceSugar {
  private val SRC_KEY = "src"
  private val COUNTERS = "Counters"
  private val NODES = "Nodes"
  private val EDGES = "Edges"

  private object MyMapper extends TypedMapper[LongWritable, Text, IntWritable, BfsNode] {
    private var src: Int = _
    private val node = new BfsNode
    private val nid = new IntWritable()

    override def setup(context: Context): Unit = {
      src = context.getConfiguration.getInt(SRC_KEY, 0)
      node.setType(BfsNode.Type.Complete)
    }

    override def map(key: LongWritable, value: Text, context: Context): Unit = {
      val arr = value.toString.trim.split("\\s+")

      val ints = arr.map(_.toInt)
      val cur = ints.head

      nid.set(cur)
      node.setNodeId(cur)
      node.setDistance(if(cur == src) 0 else Int.MaxValue)

      node.setAdjacencyList(new ArrayListOfIntsWritable(ints.tail))

      context.getCounter(COUNTERS, NODES).increment(1)
      context.getCounter(COUNTERS, EDGES).increment(arr.length - 1)

      context.write(nid, node)
    }
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)
    lazy val src = opt[Int](descr = "src", required = true)

    mainOptions = Seq(input, output, src)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Tool name: " + this.getClass.getName)
    log.info(" - inputDir: " + args.input())
    log.info(" - outputDir: " + args.output())
    log.info(" - src: " + args.src())

    val conf = getConf
    conf.setInt(SRC_KEY, args.src())
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024)

    val thisJob =
      job(s"EncodeBfsGraph[input: ${args.input()}, output: ${args.output()}, src: ${args.src()}", conf)
        .textFile(args.input())
        .map(MyMapper)
        .reduce(0)

    time {
      thisJob.saveAsSequenceFile(args.output(), deleteExisting = true)
    }

    0
  }
}
