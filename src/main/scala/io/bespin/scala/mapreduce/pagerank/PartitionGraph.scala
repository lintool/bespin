package io.bespin.scala.mapreduce.pagerank

import io.bespin.java.mapreduce.pagerank.{NonSplitableSequenceFileInputFormat, PageRankNode}
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IntWritable
import org.rogach.scallop.ScallopConf

/**
  * <p>
  * Driver program for partitioning the graph.
  * </p>
  */
object PartitionGraph extends BaseConfiguredTool with MapReduceSugar {

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)
    lazy val numNodes = opt[Int](descr = "number of nodes", required = true)
    lazy val numPartitions = opt[Int](descr = "number of partitions", required = true)
    lazy val range = opt[Boolean](descr = "use range partitioner", required = false, default = Some(false))

    mainOptions = Seq(input, output, numNodes, numPartitions, range)
  }

  /**
    * Create an instance of the RangePartitioner class for PageRankNodes
    */
  object ThisRangePartitioner extends RangePartitioner[PageRankNode]

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Tool name: " + PartitionGraph.getClass.getSimpleName)
    log.info(" - input dir: " + args.input())
    log.info(" - output dir: " + args.output())
    log.info(" - num partitions: " + args.numPartitions())
    log.info(" - node cnt: " + args.numNodes())
    log.info(" - use range partitioner: " + args.range())

    val conf = getConf
    conf.setInt("NodeCount", args.numNodes())

    val input =
      job(PartitionGraph.getClass.getSimpleName + ":" + args.input(), conf)
        .file(new Path(args.input()), classOf[NonSplitableSequenceFileInputFormat[IntWritable, PageRankNode]])

    val partitionedJob =
      if(args.range()) {
        input.partition(ThisRangePartitioner)
      } else {
        input
      }

    val reducedJob = partitionedJob.reduce(args.numPartitions())

    time {
      reducedJob.saveAsSequenceFile(new Path(args.output()), deleteExisting = true)
    }

    0
  }
}
