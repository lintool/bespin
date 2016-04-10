package io.bespin.scala.util

import java.net.{URI, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, Suite}

import scala.collection.mutable
import scala.io.Source

sealed trait WithTempOutputDir { self: Suite =>
  protected val outputDir = URI.create(System.getProperty("java.io.tmpdir") + "/" + suiteName).toString
  protected def resultDir = outputDir
}

sealed trait SingleJobFixture extends WithTempOutputDir with BeforeAndAfterAll { self: Suite with TestLogging =>
  protected def initialJob: Any

  override def beforeAll = {
    log.info(s"Running initial job for suite $suiteName")
    initialJob
    log.info(s"Initial job for $suiteName complete.")
  }

  override final def afterAll = {
    FileSystem.get(new Configuration()).delete(new Path(outputDir), true)
  }
}

sealed trait IterableJobFixture extends SingleJobFixture { self: Suite with TestLogging =>
  protected def iterations: Int

  protected def iterJob(iter: Int): Any

  override def beforeAll = {
    super.beforeAll
    for(i <- 0 until iterations) {
      log.info(s"Running iteration $i of initial job for suite $suiteName")
      iterJob(i)
      log.info(s"Iteration $i of initial job for suite $suiteName")
    }
  }
}

sealed trait KVDictJobFixture[K, V] extends WithTempOutputDir with BeforeAndAfterAll { self: Suite with TestLogging =>
  protected def tupleConv(key: String, value: String): (K, V)

  private val valueDict: scala.collection.mutable.Map[K, V] = new mutable.HashMap()

  final def programOutput(f: collection.Map[K, V] => Any) {
    f(valueDict)
  }

  override def beforeAll = {
    super.beforeAll
    FileSystem.get(new Configuration()).listStatus(new Path(resultDir))
      .withFilter(_.getPath.getName.startsWith("part-"))
      .foreach { s =>
        val fileStream = FileSystem.get(new Configuration()).open(s.getPath)
        val reader = Source.fromInputStream(fileStream)
        reader.getLines().foreach { line =>
          val res = line.split("\t")
          val conv = tupleConv(res(0), res(1))
          valueDict.put(conv._1, conv._2)
        }
      }
  }
}

trait SingleKVTest[K,V] extends SingleJobFixture with KVDictJobFixture[K,V] { self: Suite with TestLogging => }
trait IterableKVTest[K,V] extends IterableJobFixture with KVDictJobFixture[K,V] { self: Suite with TestLogging => }

trait WithExternalFile extends BeforeAndAfterAll { self: Suite with TestLogging =>
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
      try {
        val source = scala.io.Source.fromURL(url)
        val fs = FileSystem.get(conf).create(path)
        while(source.hasNext) {
          fs.write(source.next)
        }
        fs.close()
        source.close()
      } catch {
        case e: Exception =>
          log.error(s"Error retrieving test file from $url: $e")
          throw e
      }
    }
    super.beforeAll
  }
}