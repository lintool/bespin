package io.bespin.scala.mapreduce

import io.bespin.scala.util._
import org.apache.hadoop.util.ToolRunner
import org.scalatest.{FlatSpec, Matchers}

sealed abstract class WordCountLocalIT(override val url: String)
  extends FlatSpec with Matchers with TestLogging with SingleKVTest[String, Long] with WithExternalFile {

  override protected def tupleConv(key: String, value: String): (String, Long) = (key, value.toLong)

  s"Wordcount:$suiteName" should "produce expected count for 'a'" in programOutput { map =>
    map("a") shouldBe 14593
  }

  it should "produce expected count for 'hamlet'" in programOutput { map =>
    map("hamlet") shouldBe 107
  }

}

class WordCountScalaIT extends WordCountLocalIT(TestConstants.Shakespeare_Url) {
  override def initialJob: Any =
    ToolRunner.run(io.bespin.scala.mapreduce.wordcount.WordCount, Array(
      "--input", filePath,
      "--output", outputDir,
      "--reducers", "1"
    ))
}

class WordCountIMCScalaIT extends WordCountLocalIT(TestConstants.Shakespeare_Url) {
  override def initialJob: Any =
    ToolRunner.run(io.bespin.scala.mapreduce.wordcount.WordCount, Array(
      "--input", filePath,
      "--output", outputDir,
      "--reducers", "1",
      "--imc"
    ))
}

class WordCountJavaIT extends WordCountLocalIT(TestConstants.Shakespeare_Url) {
  override def initialJob: Any =
    ToolRunner.run(new io.bespin.java.mapreduce.wordcount.WordCount, Array(
      "-input", filePath,
      "-output", outputDir,
      "-reducers", "1"
    ))
}

class WordCountIMCJavaIT extends WordCountLocalIT(TestConstants.Shakespeare_Url) {
  override def initialJob: Any =
    ToolRunner.run(new io.bespin.java.mapreduce.wordcount.WordCount, Array(
      "-input", filePath,
      "-output", outputDir,
      "-reducers", "1",
      "-imc"
    ))
}