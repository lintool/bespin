package io.bespin.scala.mapreduce

import io.bespin.scala.util._
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{FlatSpec, Matchers}

sealed abstract class CoocurrenceStripesLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with SingleKVTest[String, Map[String, Long]] with WithExternalFile {

  private val tupleRegex = "(.*)=(.*)".r
  override protected def tupleConv(key: String, value: String): (String, Map[String, Long]) = {
    val map = value.stripPrefix("{").stripSuffix("}").split(", ").map(_.trim).collect {
      case tupleRegex(l, r) => l -> r.toLong
    }.toMap
    (key, map)
  }

  s"Cooccurrence-stripes:$suiteName" should "get correct co-occurrences for (the, of)" in programOutput { map =>
    map("the")("of") shouldBe 5974
  }

  it should "get correct co-occurrences for (the, and)" in programOutput { map =>
    map("the")("and") shouldBe 2923
  }

  it should "get correct co-occurrences for (i, am)" in programOutput { map =>
    map("i")("am") shouldBe 2110
  }

  it should "get correct co-occurrences for (poor, yorick)" in programOutput { map =>
    map("poor")("yorick") shouldBe 1
  }

  it should "get correct co-occurrences for (slings, arrows)" in programOutput { map =>
    map("slings")("arrows") shouldBe 1
  }

  it should "get correct total count for co-occurrences with (dream *)" in programOutput { map =>
    map("dream").values.sum shouldBe 374
  }

}

class CoocurrenceStripesScalaIT extends CoocurrenceStripesLocalIT(TestConstants.Shakespeare_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(io.bespin.scala.mapreduce.cooccur.ComputeCooccurrenceMatrixStripes, Array(
      "--input", filePath,
      "--output", outputDir,
      "--window", "2",
      "--reducers", "1"
    ))
}

class CoocurrenceStripesJavaIT extends CoocurrenceStripesLocalIT(TestConstants.Shakespeare_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(new io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixStripes, Array(
      "-input", filePath,
      "-output", outputDir,
      "-window", "2",
      "-reducers", "1"
    ))
}
