package io.bespin.scala.mapreduce.cooccur

import org.rogach.scallop.ScallopConf

class Conf(args: Seq[String]) extends ScallopConf(args) {
  lazy val input = opt[String](descr = "input path", required = true)
  lazy val output = opt[String](descr = "output path", required = true)
  lazy val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  lazy val window = opt[Int](descr = "cooccurrence window", required = false, default = Some(2))

  mainOptions = Seq(input, output, reducers, window)
}