package io.bespin.scala.mapreduce.search

import java.io.{InputStreamReader, BufferedReader}

import io.bespin.scala.util.WritableConversions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.{IntWritable, MapFile}
import org.rogach.scallop.ScallopConf
import tl.lin.data.array.ArrayListWritable
import tl.lin.data.fd.Int2IntFrequencyDistributionEntry
import tl.lin.data.pair.{PairOfWritables, PairOfInts}

import scala.collection.JavaConverters._

object LookupPostings extends WritableConversions {

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val index = opt[String](descr = "index path",  required = true)
    lazy val collection = opt[String](descr = "collection path", required = true)
    lazy val term = opt[String](descr = "term", required = true)

    mainOptions = Seq(index, collection, term)
  }

  private def lookupTerm(term: String, reader: MapFile.Reader, collectionPath: String, fs: FileSystem): Unit = {
    val collection = fs.open(new Path(collectionPath))

    val value = new PairOfWritables[IntWritable, ArrayListWritable[PairOfInts]]()
    val w = Option(reader.get(term, value))

    w match {
      case None =>
        println(s"\nThe term '$term' does not appear in the collection")

      case Some(t) =>
        val postings = value.getRightElement
        println(s"\nComplete postings list for '$term':")
        println("df = " + value.getLeftElement)

        val hist = new Int2IntFrequencyDistributionEntry()
        postings.iterator().asScala.foreach { pair =>
          hist.increment(pair.getRightElement)
          print(pair)
          collection.seek(pair.getLeftElement)
          val r = new BufferedReader(new InputStreamReader(collection))

          val d = r.readLine()
          if(d.length > 80)
            println(s": ${d.substring(0, 80)}...")
          else
            println(s": $d")
        }

        println(s"\nHistogram of tf values for '$term'")
        hist.iterator().asScala.foreach(pair => println(pair.getLeftElement + "\t" + pair.getRightElement))
        collection.close()
    }

  }

  def main(argv: Array[String]): Unit = {
    val args = new Conf(argv)

    if(args.collection().endsWith(".gz")) {
      println("gzipped collection is not seekable: use compressed version!")
      System.exit(-1)
    }

    val config = new Configuration()
    val fs = FileSystem.get(config)
    val reader = new MapFile.Reader(new Path(args.index() + "/part-r-00000"), config)

    lookupTerm(args.term(), reader, args.collection(), fs)

    reader.close()
  }

}
