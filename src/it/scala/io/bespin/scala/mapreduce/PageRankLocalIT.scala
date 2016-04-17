package io.bespin.scala.mapreduce

import io.bespin.scala.util.{SingleKVTest, TestConstants, TestLogging, WithExternalFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{FlatSpec, Matchers}

sealed abstract class PageRankLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with SingleKVTest[Int, Float] with WithExternalFile {

  override protected def tupleConv(key: String, value: String): (Int, Float) = {
    (key.toInt, value.toFloat)
  }

  protected val Epsilon = 0.0001f
  protected val pageRankIterations = 10
  protected val expectedNodes = 6301
  protected val pageRankRecordsDir = outputDir + "/PageRankRecords/"
  protected val pageRankIterDir = outputDir + "/PageRankIter/"
  protected def iterDir(i: Int): String = pageRankIterDir + "iter%04d/".format(i)
  protected override val resultDir = outputDir + "/PageRankTop20/"

  s"PageRank:$suiteName" should "contain the correctly-extracted top 20 nodes" in programOutput { map =>
    map.size shouldBe 20
  }

  it should "return correct PageRank value for node 367" in programOutput { map =>
    map(367) shouldEqual  -6.037346f +- Epsilon
  }

  it should "return correct PageRank value for node 249" in programOutput { map =>
    map(249) shouldEqual -6.126378f +- Epsilon
  }

  it should "return correct PageRank value for node 145" in programOutput { map =>
    map(145) shouldEqual -6.187434f +- Epsilon
  }

  it should "return correct PageRank value for node 264" in programOutput { map =>
    map(264) shouldEqual -6.2151237f +- Epsilon
  }

  it should "return correct PageRank value for node 266" in programOutput { map =>
    map(266) shouldEqual -6.2329774f +- Epsilon
  }

  it should "return correct PageRank value for node 4" in programOutput { map =>
    map(4) shouldEqual -6.4705563f +- Epsilon
  }

  it should "return correct PageRank value for node 7" in programOutput { map =>
    map(7) shouldEqual -6.501463f +- Epsilon
  }
}

abstract sealed class PageRankScala(url: String) extends PageRankLocalIT(url) {
  def setup(rangePartition: Boolean = false): Unit = {
    ToolRunner.run(io.bespin.scala.mapreduce.pagerank.BuildPageRankRecords, Array(
      "--input", filePath,
      "--output", pageRankRecordsDir,
      "--num-nodes", expectedNodes.toString
    ))
    FileSystem.get(new Configuration()).mkdirs(new Path(pageRankIterDir))
    if(rangePartition) {
      ToolRunner.run(io.bespin.scala.mapreduce.pagerank.PartitionGraph, Array(
        "--input", pageRankRecordsDir,
        "--output", iterDir(0),
        "--num-partitions", "5",
        "--num-nodes", expectedNodes.toString,
        "--range"
      ))
    } else {
      ToolRunner.run(io.bespin.scala.mapreduce.pagerank.PartitionGraph, Array(
        "--input", pageRankRecordsDir,
        "--output", iterDir(0),
        "--num-partitions", "5",
        "--num-nodes", expectedNodes.toString
      ))
    }
  }

  def countMax = {
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.FindMaxPageRankNodes, Array(
      "-input", iterDir(pageRankIterations),
      "-output", resultDir,
      "-top", "20"
    ))
  }
}

abstract sealed class PageRankJava(url: String) extends PageRankLocalIT(url) {
  def setup(rangePartition: Boolean = false): Unit = {
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.BuildPageRankRecords, Array(
      "-input", filePath,
      "-output", pageRankRecordsDir,
      "-numNodes", expectedNodes.toString
    ))
    FileSystem.get(new Configuration()).mkdirs(new Path(pageRankIterDir))
    if(rangePartition) {
      ToolRunner.run(new io.bespin.java.mapreduce.pagerank.PartitionGraph, Array(
        "-input", pageRankRecordsDir,
        "-output", iterDir(0),
        "-numPartitions", "5",
        "-numNodes", expectedNodes.toString,
        "-range"
      ))
    } else {
      ToolRunner.run(new io.bespin.java.mapreduce.pagerank.PartitionGraph, Array(
        "-input", pageRankRecordsDir,
        "-output", iterDir(0),
        "-numPartitions", "5",
        "-numNodes", expectedNodes.toString
      ))
    }
  }

  def countMax = {
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.FindMaxPageRankNodes, Array(
      "-input", iterDir(pageRankIterations),
      "-output", resultDir,
      "-top", "20"
    ))
  }
}

class PageRankScalaIT extends PageRankScala(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup()
    ToolRunner.run(io.bespin.scala.mapreduce.pagerank.RunPageRankBasic, Array(
      "--base", pageRankIterDir,
      "--start", "0",
      "--end", pageRankIterations.toString,
      "--num-nodes", expectedNodes.toString
    ))
    countMax
  }
}

class PageRankJavaIT extends PageRankJava(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup()
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.RunPageRankBasic, Array(
      "-base", pageRankIterDir,
      "-start", "0",
      "-end", pageRankIterations.toString,
      "-numNodes", expectedNodes.toString
    ))
    countMax
  }
}

class PageRankRangeScalaIT extends PageRankScala(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup(rangePartition = true)
    ToolRunner.run(io.bespin.scala.mapreduce.pagerank.RunPageRankBasic, Array(
      "--base", pageRankIterDir,
      "--start", "0",
      "--end", pageRankIterations.toString,
      "--num-nodes", expectedNodes.toString,
      "--range"
    ))
    countMax
  }
}

class PageRankRangeJavaIT extends PageRankJava(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup(rangePartition = true)
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.RunPageRankBasic, Array(
      "-base", pageRankIterDir,
      "-start", "0",
      "-end", pageRankIterations.toString,
      "-numNodes", expectedNodes.toString,
      "-range"
    ))
    countMax
  }
}

class PageRankIMCScalaIT extends PageRankScala(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup()
    ToolRunner.run(io.bespin.scala.mapreduce.pagerank.RunPageRankBasic, Array(
      "--base", pageRankIterDir,
      "--start", "0",
      "--end", pageRankIterations.toString,
      "--num-nodes", expectedNodes.toString,
      "--use-in-mapper-combiner"
    ))
    countMax
  }
}

class PageRankIMCJavaIT extends PageRankJava(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup()
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.RunPageRankBasic, Array(
      "-base", pageRankIterDir,
      "-start", "0",
      "-end", pageRankIterations.toString,
      "-numNodes", expectedNodes.toString,
      "-useInMapperCombiner"
    ))
    countMax
  }
}

class PageRankCombinerScalaIT extends PageRankScala(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup()
    ToolRunner.run(io.bespin.scala.mapreduce.pagerank.RunPageRankBasic, Array(
      "--base", pageRankIterDir,
      "--start", "0",
      "--end", pageRankIterations.toString,
      "--num-nodes", expectedNodes.toString,
      "--use-combiner"
    ))
    countMax
  }
}

class PageRankCombinerJavaIT extends PageRankJava(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup()
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.RunPageRankBasic, Array(
      "-base", pageRankIterDir,
      "-start", "0",
      "-end", pageRankIterations.toString,
      "-numNodes", expectedNodes.toString,
      "-useCombiner"
    ))
    countMax
  }
}

class PageRankEverythingScalaIT extends PageRankScala(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup(true)
    ToolRunner.run(io.bespin.scala.mapreduce.pagerank.RunPageRankBasic, Array(
      "--base", pageRankIterDir,
      "--start", "0",
      "--end", pageRankIterations.toString,
      "--num-nodes", expectedNodes.toString,
      "--use-combiner",
      "--use-in-mapper-combiner",
      "--range"
    ))
    countMax
  }
}

class PageRankEverythingJavaIT extends PageRankJava(TestConstants.Graph_Url) {
  override protected def initialJob = {
    setup(true)
    ToolRunner.run(new io.bespin.java.mapreduce.pagerank.RunPageRankBasic, Array(
      "-base", pageRankIterDir,
      "-start", "0",
      "-end", pageRankIterations.toString,
      "-numNodes", expectedNodes.toString,
      "-useCombiner",
      "-useInMapperCombiner",
      "-range"
    ))
    countMax
  }
}