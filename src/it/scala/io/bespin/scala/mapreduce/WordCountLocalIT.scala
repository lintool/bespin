package io.bespin.scala.mapreduce

import io.bespin.scala.mapreduce.wordcount.WordCount
import org.scalatest.{FlatSpec, Matchers}

class WordCountLocalIT extends FlatSpec with Matchers with KVJobFixture[String, Long] {

  override def initialJob(outputDir: String): Any = WordCount.run(Array(
    "--input", getClass.getResource("/Shakespeare.txt").toString,
    "--output", outputDir,
    "--reducers", "1"
  ))

  override def tupleConv(key: String, value: String): (String, Long) = (key, value.toLong)

  "Wordcount" should "produce expected count for 'a'" in programOutput { map =>
    map("a") shouldBe 14593
  }

  "Wordcount" should "produce expected count for 'hamlet'" in programOutput { map =>
    map("hamlet") shouldBe 107
  }

}
