package io.bespin.scala.util

import java.util.StringTokenizer

import scala.collection.JavaConverters._

trait Tokenizer {

  def tokenize(s: String): Seq[String] = {
    new StringTokenizer(s).asScala
      .map(_.asInstanceOf[String]
        .toLowerCase()
        .replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
      .withFilter(_.nonEmpty)
      .toSeq
  }

}