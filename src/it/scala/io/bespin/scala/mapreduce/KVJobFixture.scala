package io.bespin.scala.mapreduce

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.mutable
import scala.io.Source


trait KVJobFixture[K, V] extends BeforeAndAfterAll { self: Suite =>
  def initialJob(outputDir: String): Any

  def tupleConv(key: String, value: String): (K, V)

  private val outputDir = URI.create(System.getProperty("java.io.tmpdir") + "/" + suiteName).toString
  private val valueDict: scala.collection.mutable.Map[K, V] = new mutable.HashMap()

  final def programOutput(f: collection.Map[K, V] => Any) {
    f(valueDict)
  }

  override final def beforeAll = {
    initialJob(outputDir)

    val fileStream = FileSystem.get(new Configuration()).open(new Path(outputDir + "/part-r-00000"))
    val reader = Source.fromInputStream(fileStream)

    reader.getLines().foreach { line =>
      val res = line.split("\t")
      val conv = tupleConv(res(0), res(1))
      valueDict.put(conv._1, conv._2)
    }
  }

  override final def afterAll = {
    FileSystem.get(new Configuration()).delete(new Path(outputDir), true)
  }


}
