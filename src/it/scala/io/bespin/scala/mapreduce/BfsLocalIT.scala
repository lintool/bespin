package io.bespin.scala.mapreduce

import io.bespin.scala.util._
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{FlatSpec, Matchers}

sealed abstract class BfsLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with IterableKVTest[Int, (Int, Int)] with WithExternalFile{

  private val tupleRegex = "\\{(\\d+) (\\d+) \\[(.*)\\]\\}".r
  override protected def tupleConv(key: String, value: String): (Int, (Int, Int)) = {
    val s = value match {
      case tupleRegex(selfNode, dist, connected) =>
        (selfNode.toInt, dist.toInt)
    }
    (key.toInt, s)
  }

  override protected val iterations: Int = 15
  override protected def resultDir: String = outputDir + s"/reachable-iter000$iterations"

  s"BFS:$suiteName" should "find all reachable nodes after 15 iterations" in programOutput { map =>
    map.size shouldBe 6028
  }

  it should "get correct distance for 367" in programOutput { map =>
    map(367)._2 shouldBe 0
  }

  it should "find correct distance for node 101" in programOutput { map =>
    map(101)._2 shouldBe 6
  }

  it should "find correct distance for node 201" in programOutput { map =>
    map(201)._2 shouldBe 8
  }

  it should "find correct distance for node 6276" in programOutput { map =>
    map(6276)._2 shouldBe 13
  }

  it should "find correct distance for node 6258" in programOutput { map =>
    map(6258)._2 shouldBe 11
  }

  it should "find correct distance for node 909" in programOutput { map =>
    map(909)._2 shouldBe 4
  }

  it should "find correct distance for node 5664" in programOutput { map =>
    map(5664)._2 shouldBe 7
  }
}

class BfsJavaIT extends BfsLocalIT(TestConstants.Graph_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(new io.bespin.java.mapreduce.bfs.EncodeBfsGraph, Array(
      "-input", filePath,
      "-output", outputDir + "/" + "iter0000",
      "-src", "367"
    ))

  override protected def iterJob(itr: Int): Any = {
    ToolRunner.run(new io.bespin.java.mapreduce.bfs.IterateBfs, Array(
      "-input", outputDir + "/" + s"iter000$itr",
      "-output", outputDir + "/" + s"iter000${itr + 1}",
      "-partitions", "5"
    ))
    ToolRunner.run(new io.bespin.java.mapreduce.bfs.FindReachableNodes, Array(
      "-input", outputDir + "/" + s"iter000${itr + 1}",
      "-output", outputDir + "/" + s"reachable-iter000${itr + 1}"
    ))
  }
}

class BfsScalaIT extends BfsLocalIT(TestConstants.Graph_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(io.bespin.scala.mapreduce.bfs.EncodeBfsGraph, Array(
      "--input", filePath,
      "--output", outputDir + "/" + "iter0000",
      "--src", "367"
    ))

  override protected def iterJob(itr: Int): Any = {
    ToolRunner.run(io.bespin.scala.mapreduce.bfs.IterateBfs, Array(
      "--input", outputDir + "/" + s"iter000$itr",
      "--output", outputDir + "/" + s"iter000${itr + 1}",
      "--partitions", "5"
    ))
    ToolRunner.run(io.bespin.scala.mapreduce.bfs.FindReachableNodes, Array(
      "--input", outputDir + "/" + s"iter000${itr + 1}",
      "--output", outputDir + "/" + s"reachable-iter000${itr + 1}"
    ))
  }
}
