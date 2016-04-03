package io.bespin.scala.mapreduce

import io.bespin.scala.util.{TestLogging, WithExternalFile}
import org.scalatest.{FlatSpec, Matchers}

abstract class BigramCountLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with WithExternalFile[String, Long] {

  override def tupleConv(key: String, value: String): (String, Long) = (key, value.toLong)

  s"BigramCount:$suiteName" should "produce expected count for 'a baboon'" in programOutput { map =>
    map("a baboon") shouldBe 1
  }

  it should "produce expected count for 'poor yorick'" in programOutput { map =>
    map("poor yorick") shouldBe 1
  }

  it should "produce expected count for 'dream again'" in programOutput { map =>
    map("dream again") shouldBe 2
  }

}

class BigramCountScalaIT extends BigramCountLocalIT("http://lintool.github.io/bespin-data/Shakespeare.txt") {
  override def initialJob(outputDir: String): Any =
    io.bespin.scala.mapreduce.bigram.BigramCount.main(Array(
      "--input", filePath,
      "--output", outputDir,
      "--reducers", "1"
    ))
}

class BigramCountJavaIT extends BigramCountLocalIT("http://lintool.github.io/bespin-data/Shakespeare.txt") {
  override def initialJob(outputDir: String): Any =
    io.bespin.java.mapreduce.bigram.BigramCount.main(Array(
      "-input", filePath,
      "-output", outputDir,
      "-reducers", "1"
    ))
}