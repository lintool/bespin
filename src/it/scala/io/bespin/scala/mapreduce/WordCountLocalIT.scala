package io.bespin.scala.mapreduce

import io.bespin.scala.util.{TestLogging, WithExternalFile}
import org.scalatest.{FlatSpec, Matchers}

abstract class WordCountLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with WithExternalFile[String, Long] {

  override def tupleConv(key: String, value: String): (String, Long) = (key, value.toLong)

  "Wordcount" should "produce expected count for 'a'" in programOutput { map =>
    map("a") shouldBe 14593
  }

  it should "produce expected count for 'hamlet'" in programOutput { map =>
    map("hamlet") shouldBe 107
  }

}

class WordCountScalaIT extends WordCountLocalIT("http://lintool.github.io/bespin-data/Shakespeare.txt") {
  override def initialJob(outputDir: String): Any =
    io.bespin.scala.mapreduce.wordcount.WordCount.main(Array(
      "--input", filePath,
      "--output", outputDir,
      "--reducers", "1"
    ))
}

class WordCountIMCScalaIT extends WordCountLocalIT("http://lintool.github.io/bespin-data/Shakespeare.txt") {
  override def initialJob(outputDir: String): Any =
    io.bespin.scala.mapreduce.wordcount.WordCount.main(Array(
      "--input", filePath,
      "--output", outputDir,
      "--reducers", "1",
      "--imc"
    ))
}

class WordCountJavaIT extends WordCountLocalIT("http://lintool.github.io/bespin-data/Shakespeare.txt") {
  override def initialJob(outputDir: String): Any =
    io.bespin.java.mapreduce.wordcount.WordCount.main(Array(
      "-input", filePath,
      "-output", outputDir,
      "-reducers", "1"
    ))
}

class WordCountIMCJavaIT extends WordCountLocalIT("http://lintool.github.io/bespin-data/Shakespeare.txt") {
  override def initialJob(outputDir: String): Any =
    io.bespin.java.mapreduce.wordcount.WordCount.main(Array(
      "-input", filePath,
      "-output", outputDir,
      "-reducers", "1",
      "-imc"
    ))
}