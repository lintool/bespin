/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package io.bespin.scala.mapreduce.mean

import io.bespin.scala.util.Tokenizer
import io.bespin.scala.util.WritableConversions

import scala.collection.JavaConverters._
import scala.collection.mutable._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner
import org.apache.log4j._
import org.rogach.scallop._
import tl.lin.data.map.HMapKL
import tl.lin.data.pair.PairOfLongs

class ComputeMeanV4aConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

/**
  * Program that computes the mean of values associated with each key (version 4a),
  * implemented in Scala.
  * This implementation illustrates the "in-mapper combining" concept with optimized HashMaps
  * and is faster than version 4.
  */
object ComputeMeanV4a extends Configured with Tool with WritableConversions with Tokenizer {
  val log = Logger.getLogger(getClass.getName)

  class MyMapper extends Mapper[Text, Text, Text, PairOfLongs] {
    val sums: HMapKL[String] = new HMapKL[String]()
    val counts: HMapKL[String] = new HMapKL[String]()

    override def setup(context: Mapper[Text, Text, Text, PairOfLongs]#Context) = {
      sums.clear()
      counts.clear()
    }

    override def map(key: Text, value: Text,
                     context: Mapper[Text, Text, Text, PairOfLongs]#Context) = {
      val k: String = key.toString
      if (sums.containsKey(k)) {
        sums.put(k, sums.get(k) + value.toString.toLong)
        counts.put(k, counts.get(k) + 1L)
      } else {
        sums.put(k, value.toString.toInt.toLong)
        counts.put(k, 1L)
      }
    }

    override def cleanup(context: Mapper[Text, Text, Text, PairOfLongs]#Context) = {
      for (key <- counts.keySet.asScala) {
        context.write(key, new PairOfLongs(sums.get(key), counts.get(key)))
      }
    }
  }

  class MyCombiner extends Reducer[Text, PairOfLongs, Text, PairOfLongs] {
    override def reduce(key: Text, values: java.lang.Iterable[PairOfLongs],
                        context: Reducer[Text, PairOfLongs, Text, PairOfLongs]#Context) = {
      var sum: Long = 0L
      var cnt: Long = 0L
      for (value <- values.asScala) {
        sum += value.getLeftElement
        cnt += value.getRightElement
      }
      context.write(key, new PairOfLongs(sum, cnt))
    }
  }

  class MyReducer extends Reducer[Text, PairOfLongs, Text, IntWritable] {
    override def reduce(key: Text, values: java.lang.Iterable[PairOfLongs],
                        context: Reducer[Text, PairOfLongs, Text, IntWritable]#Context) = {
      var sum: Long = 0L
      var cnt: Long = 0L
      for (value <- values.asScala) {
        sum += value.getLeftElement
        cnt += value.getRightElement
      }
      context.write(key, (sum/cnt).toInt)
    }
  }

  override def run(argv: Array[String]) : Int = {
    val args = new ComputeMeanV4aConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = getConf()
    val job = Job.getInstance(conf)

    FileInputFormat.addInputPath(job, new Path(args.input()))
    FileOutputFormat.setOutputPath(job, new Path(args.output()))

    job.setJobName(this.getClass.getSimpleName)
    job.setJarByClass(this.getClass)

    job.setInputFormatClass(classOf[KeyValueTextInputFormat])
    job.getConfiguration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t")
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[PairOfLongs])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    job.setMapperClass(classOf[MyMapper])
    job.setReducerClass(classOf[MyReducer])

    job.setNumReduceTasks(args.reducers())

    val outputDir = new Path(args.output())
    FileSystem.get(conf).delete(outputDir, true)

    val startTime = System.currentTimeMillis()
    job.waitForCompletion(true)
    log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")

    return 0
  }

  def main(args: Array[String]) {
    ToolRunner.run(this, args)
  }
}
