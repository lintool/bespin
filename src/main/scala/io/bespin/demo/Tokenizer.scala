package io.bespin.demo;

import java.util.StringTokenizer

import scala.collection.JavaConversions._

trait Tokenizer {
  def tokenize(s: String): List[String] = {
    new StringTokenizer(s).toList
      .map(_.asInstanceOf[String].toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", ""))
      .filter(_.length != 0)
  }
}