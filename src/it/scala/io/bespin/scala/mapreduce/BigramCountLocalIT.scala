package io.bespin.scala.mapreduce

import io.bespin.scala.util._
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{FlatSpec, Matchers}

sealed abstract class BigramCountLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with SingleKVTest[String, Long] with WithExternalFile {

  override protected def tupleConv(key: String, value: String): (String, Long) = (key, value.toLong)

  s"BigramCount:$suiteName" should "produce expected count for 'a baboon'" in programOutput { map =>
    map("a baboon") shouldBe 1
  }

  it should "produce expected count for 'poor yorick'" in programOutput { map =>
    map("poor yorick") shouldBe 1
  }

  it should "produce expected count for 'dream again'" in programOutput { map =>
    map("dream again") shouldBe 2
  }

  it should "produce expected count for 'dream away'" in programOutput { map =>
    map("dream away") shouldBe 2
  }

  it should "produce expected count for 'dream as'" in programOutput { map =>
    map("dream as") shouldBe 1
  }

}

class BigramCountScalaIT extends BigramCountLocalIT(TestConstants.Shakespeare_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(io.bespin.scala.mapreduce.bigram.BigramCount, Array(
      "--input", filePath,
      "--output", outputDir,
      "--reducers", "1"
    ))
}

class BigramCountJavaIT extends BigramCountLocalIT(TestConstants.Shakespeare_Url) {
  override protected def initialJob: Any =
    ToolRunner.run(new io.bespin.java.mapreduce.bigram.BigramCount, Array(
      "-input", filePath,
      "-output", outputDir,
      "-reducers", "1"
    ))
}