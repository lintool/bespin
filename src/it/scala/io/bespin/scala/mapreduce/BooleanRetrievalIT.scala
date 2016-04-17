package io.bespin.scala.mapreduce

import java.io.{ByteArrayInputStream, PrintStream}

import io.bespin.scala.util.{TestConstants, TestLogging, WithExternalFile, WithTempOutputDir}
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.immutable.TreeMap

abstract sealed class BooleanRetrievalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging
    with BeforeAndAfterAll with WithTempOutputDir with WithExternalFile {

  protected final case class RetrievalResult(occurrences: TreeMap[Int, String])

  protected def runQuery(queryString: String): Unit

  private val resultRegex = "(\\d+)\t(.*)".r

  private def retrievalOutput(queryString: String)(f: RetrievalResult => Any): Unit = {
    val stream = new java.io.ByteArrayOutputStream()
    val originalOutStream = System.out
    val interceptionStream = new PrintStream(stream)
    // Temporarily redirect stdout to a stream we can capture
    System.setOut(interceptionStream)
    Console.withOut(stream) {
      runQuery(queryString)
    }
    System.setOut(originalOutStream)
    val input = new ByteArrayInputStream(stream.toByteArray)

    val builder = TreeMap.newBuilder[Int, String]
    scala.io.Source.fromInputStream(input).getLines().foreach {
      case resultRegex(num, str) => builder += ((num.toInt, str.trim))
      case _ =>
    }

    f(RetrievalResult(builder.result()))
  }


  s"BooleanRetrieval:$suiteName" should "produce expected count for 'poor'" in retrievalOutput("poor") { results =>
    results.occurrences.size shouldBe 623
  }

  it should "produce expected count for 'white red OR rose AND pluck AND'" in retrievalOutput("white red OR rose AND pluck AND") { results =>
    results.occurrences.size shouldBe 5
  }

  it should "produce expected lines for 'white red OR rose AND pluck AND'" in retrievalOutput("white red OR rose AND pluck AND") { results =>
    results.occurrences.values.head shouldBe "From off this brier pluck a white rose with me."
  }

  it should "produce expected lines for 'poor yorick AND'" in retrievalOutput("poor yorick AND") { results =>
    results.occurrences.size shouldBe 1
    results.occurrences.values.head should include ("Alas, poor Yorick!")
  }

  it should "produce nothing for query with no result" in retrievalOutput("romeo hamlet AND") { results =>
    results.occurrences shouldBe empty
  }

  it should "produce expected lines for 'romeo juliet AND'" in retrievalOutput("romeo juliet AND") { results =>
    results.occurrences.values.head should startWith ("THE TRAGEDY")
  }

}

class BooleanRetrievalScalaIT extends BooleanRetrievalIT(TestConstants.Shakespeare_Url) {
  override def beforeAll = {
    super.beforeAll
    ToolRunner.run(io.bespin.scala.mapreduce.search.BuildInvertedIndex, Array(
      "--input", filePath,
      "--output", outputDir + "/index"
    ))
  }

  override protected def runQuery(queryString: String): Unit =
    ToolRunner.run(io.bespin.scala.mapreduce.search.BooleanRetrieval, Array(
      "--index", outputDir + "/index",
      "--collection", filePath,
      "--query", queryString
    ))
}

class BooleanRetrievalJavaIT extends BooleanRetrievalIT(TestConstants.Shakespeare_Url) {
  override def beforeAll = {
    super.beforeAll
    ToolRunner.run(new io.bespin.java.mapreduce.search.BuildInvertedIndex, Array(
      "-input", filePath,
      "-output", outputDir + "/index"
    ))
  }

  override protected def runQuery(queryString: String): Unit =
    ToolRunner.run(new io.bespin.java.mapreduce.search.BooleanRetrieval, Array(
      "-index", outputDir + "/index",
      "-collection", filePath,
      "-query", queryString
    ))
}
