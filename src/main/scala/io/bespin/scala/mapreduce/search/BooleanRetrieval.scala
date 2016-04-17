package io.bespin.scala.mapreduce.search

import java.io.{InputStreamReader, BufferedReader}

import io.bespin.scala.mapreduce.util.{MapReduceSugar, BaseConfiguredTool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FSDataInputStream, FileSystem}
import org.apache.hadoop.io.{Text, IntWritable, MapFile}
import org.rogach.scallop.ScallopConf
import tl.lin.data.array.ArrayListWritable
import tl.lin.data.pair.{PairOfWritables, PairOfInts}

import scala.collection.immutable.SortedSet
import scala.collection.mutable

import scala.collection.JavaConverters._

object BooleanRetrieval extends BaseConfiguredTool with MapReduceSugar {

  private def runQuery(q: String, index: MapFile.Reader, collection: FSDataInputStream): Unit = {
    val terms = q.split("""\s+""")

    val stack = new mutable.Stack[SortedSet[Int]]()
    terms.foreach {
      case "AND" => performAND(stack)
      case "OR" => performOR(stack)
      case t => pushTerm(t, stack, index)
    }

    val result = stack.pop()
    result.foreach { i =>
      val line = fetchLine(i, collection)
      println(s"$i\t$line")
    }
  }

  private def pushTerm(term: String, stack: mutable.Stack[SortedSet[Int]], index: MapFile.Reader): Unit = {
    stack.push(fetchDocumentSet(term, index))
  }

  private def performOR(stack: mutable.Stack[SortedSet[Int]]): Unit = {
    val s1 = stack.pop()
    val s2 = stack.pop()

    stack.push(s1.union(s2))
  }

  private def performAND(stack: mutable.Stack[SortedSet[Int]]): Unit = {
    val s1 = stack.pop()
    val s2 = stack.pop()

    stack.push(s1.intersect(s2))
  }

  private def fetchDocumentSet(term: String, index: MapFile.Reader): SortedSet[Int] = {
    val set = SortedSet.newBuilder[Int]
    fetchPostings(term, index).asScala.foreach(p => set += p.getLeftElement)
    set.result()
  }

  private def fetchPostings(term: String, index: MapFile.Reader): ArrayListWritable[PairOfInts] = {
    val key: Text = term
    val value: PairOfWritables[IntWritable, ArrayListWritable[PairOfInts]] = new PairOfWritables()
    index.get(key, value)
    value.getRightElement
  }

  private def fetchLine(offset: Long, collection: FSDataInputStream): String = {
    collection.seek(offset)
    val reader = new BufferedReader(new InputStreamReader(collection))

    val d = reader.readLine()
    if(d.length > 80)
      d.substring(0, 80) + "..."
    else
      d
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val index = opt[String](descr = "index path",  required = true)
    lazy val collection = opt[String](descr = "collection path", required = true)
    lazy val query = opt[String](descr = "query", required = true)

    mainOptions = Seq(index, collection, query)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    if(args.collection().endsWith(".gz")) {
      log.error("gzipped collection is not seekable: use compresed version!")
      -1
    } else {

      val fs = FileSystem.get(new Configuration())
      val index = new MapFile.Reader(new Path(args.index() + "/part-r-00000"), fs.getConf)
      val collection = fs.open(new Path(args.collection()))

      println("Query: " + args.query())
      val startTime = System.currentTimeMillis()

      runQuery(args.query(), index, collection)

      println("\nquery completed in " + (System.currentTimeMillis() - startTime) + " ms")

      index.close()
      collection.close()

      1
    }
  }
}
