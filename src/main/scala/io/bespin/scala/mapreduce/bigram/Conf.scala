package io.bespin.scala.mapreduce.bigram

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {
  lazy val input = opt[String](descr = "input path", required = true)
  lazy val output = opt[String](descr = "output path", required = true)
  lazy val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))

  mainOptions = Seq(input, output, reducers)
}

class ConfWithOutput(args: Seq[String]) extends ScallopConf(args) {
  lazy val input = opt[String](descr = "input path", required = true)
  lazy val output = opt[String](descr = "output path", required = true)
  lazy val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  lazy val textOutput = toggle("textOutput", default = Some(false),
    descrYes = "Use TextOutputFormat", descrNo = "Use SequenceFileOutputFormat")
  mainOptions = Seq(input, output, reducers, textOutput)
}