package io.bespin.scala.util

import java.net.{URI, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.mutable
import scala.io.Source

trait InitialJobFixture extends BeforeAndAfterAll { self: Suite with TestLogging =>
  def initialJob(outputDir: String): Any

  protected val outputDir = URI.create(System.getProperty("java.io.tmpdir") + "/" + suiteName).toString

  override def beforeAll = {
    log.info(s"Running initial job for suite $suiteName")
    initialJob(outputDir)
    log.info(s"Initial job for $suiteName complete.")
  }

}

trait KVDictJobFixture[K, V] extends InitialJobFixture { self: Suite with TestLogging =>
  def tupleConv(key: String, value: String): (K, V)

  private val valueDict: scala.collection.mutable.Map[K, V] = new mutable.HashMap()

  final def programOutput(f: collection.Map[K, V] => Any) {
    f(valueDict)
  }

  override def beforeAll = {
    super.beforeAll
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

trait WithExternalFile[K,V] extends KVDictJobFixture[K, V] { self: Suite with TestLogging =>
  def url: String
  private lazy val urlObj = new URL(url)
  private lazy val fileDir = URI.create(System.getProperty("java.io.tmpdir"))

  lazy val filePath = fileDir.getPath + "/" + urlObj.getFile

  override final def beforeAll = {
    val conf = new Configuration()
    val path = new Path(filePath)
    if(FileSystem.get(conf).exists(path)) {
      log.info(s"Found existing data at $filePath.")
    } else {
      log.info(s"Existing data not found, fetching $url.")
      val source = scala.io.Source.fromURL(url)
      val fs = FileSystem.get(conf).create(path)
      while(source.hasNext) {
        fs.write(source.next)
      }
      fs.close()
      source.close()
    }
    super.beforeAll
  }
}