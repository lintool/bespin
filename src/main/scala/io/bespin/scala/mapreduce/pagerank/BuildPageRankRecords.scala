package io.bespin.scala.mapreduce.pagerank

import io.bespin.java.mapreduce.pagerank.PageRankNode
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.rogach.scallop.ScallopConf
import tl.lin.data.array.ArrayListOfIntsWritable


/**
  * <p>
  * Driver program that takes a plain-text encoding of a directed graph and builds corresponding
  * Hadoop structures for representing the graph.
  * </p>
  *
  * @author Jimmy Lin
  * @author Michael Schatz
  */
object BuildPageRankRecords extends BaseConfiguredTool with MapReduceSugar {

  private val NODE_CNT_FIELD: String = "node.cnt"

  private val GRAPH_QUAL: String = "graph"
  private val NUM_NODES: String = "numNodes"
  private val NUM_EDGES: String = "numEdges"
  private val NUM_ACTIVE_NODES: String = "numActiveNodes"

  private object MyMapper extends TypedMapper[LongWritable, Text, IntWritable, PageRankNode] {
    private val nid: IntWritable = new IntWritable()
    private val node: PageRankNode = new PageRankNode()

    override def setup(context: Context): Unit = {
      val n = context.getConfiguration.getInt(NODE_CNT_FIELD, 0)
      if(n == 0)
        throw new RuntimeException(s"$NODE_CNT_FIELD must be set to non-zero value!")
      node.setType(PageRankNode.Type.Complete)
      node.setPageRank(-StrictMath.log(n).toFloat)
    }


    override def map(key: LongWritable, value: Text, context: Context): Unit = {
      val arr: Array[String] = value.toString.trim.split("\\s+")

      val nodeId = arr(0).toInt
      nid.set(nodeId)
      node.setNodeId(nodeId)
      if(arr.length == 1) {
        node.setAdjacencyList(new ArrayListOfIntsWritable())
      } else {
        val neighbors: Array[Int] = arr.tail.map(_.toInt)
        node.setAdjacencyList(new ArrayListOfIntsWritable(neighbors))
      }

      context.getCounter(GRAPH_QUAL, NUM_NODES).increment(1)
      context.getCounter(GRAPH_QUAL, NUM_EDGES).increment(arr.length - 1)

      if(arr.length > 1)
        context.getCounter(GRAPH_QUAL, NUM_ACTIVE_NODES).increment(1)

      context.write(nid, node)
    }

  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)
    lazy val numNodes = opt[Int](descr = "number of nodes", required = true)

    mainOptions = Seq(input, output, numNodes)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info(s"Tool name: ${BuildPageRankRecords.getClass.getSimpleName}")
    log.info(s" - inputDir: ${args.input()}")
    log.info(s" - outputDir: ${args.output()}")
    log.info(s" - numNodes: ${args.numNodes()}")

    val inputPath = new Path(args.input())
    val outputPath = new Path(args.output())

    val conf = getConf
    conf.setInt(NODE_CNT_FIELD, args.numNodes())
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024)

    val thisJob =
      job(this.getClass.getSimpleName, conf)
        .textFile(inputPath)
        .map(MyMapper)
        .reduce(0)

    time {
      thisJob.saveAsSequenceFile(outputPath, deleteExisting = true)
    }

    0
  }
}
