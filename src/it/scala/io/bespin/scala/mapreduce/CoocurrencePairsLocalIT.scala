package io.bespin.scala.mapreduce

import io.bespin.scala.util._
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{FlatSpec, Matchers}

sealed abstract class CoocurrencePairsLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with SingleKVTest[(String, String), Long] with WithExternalFile {

  private val tupleRegex = "\\((.*), (.*)\\)".r
  override protected def tupleConv(key: String, value: String): ((String, String), Long) = key match {
    case tupleRegex(l, r) => ((l, r), value.toLong)
  }

  s"Cooccurrence-pairs:$suiteName" should "get correct co-occurrences for (the, of)" in programOutput { map =>
    map(("the", "of")) shouldBe 5974
  }

  it should "get correct co-occurrences for (the, and)" in programOutput { map =>
    map(("the", "and")) shouldBe 2923
  }

  it should "get correct co-occurrences for (i, am)" in programOutput { map =>
    map(("i", "am")) shouldBe 2110
  }

  it should "get correct co-occurrences for (poor, yorick)" in programOutput { map =>
    map(("poor", "yorick")) shouldBe 1
  }

  it should "get correct co-occurrences for (slings, arrows)" in programOutput { map =>
    map(("slings", "arrows")) shouldBe 1
  }

}

class CoocurrencePairsScalaIT extends CoocurrencePairsLocalIT(TestConstants.Shakespeare_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(io.bespin.scala.mapreduce.cooccur.ComputeCooccurrenceMatrixPairs, Array(
      "--input", filePath,
      "--output", outputDir,
      "--window", "2",
      "--reducers", "1"
    ))
}

class CoocurrencePairsJavaIT extends CoocurrencePairsLocalIT(TestConstants.Shakespeare_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(new io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixPairs, Array(
      "-input", filePath,
      "-output", outputDir,
      "-window", "2",
      "-reducers", "1"
    ))
}