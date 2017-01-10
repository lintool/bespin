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

package io.bespin.scala.mapreduce.cooccur

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
import tl.lin.data.map.HMapStIW

class ConfStripes(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val window = opt[Int](descr = "cooccurrence window", required = false, default = Some(2))
  verify()
}

object ComputeCooccurrenceMatrixStripes extends Configured with Tool with WritableConversions with Tokenizer {
  val log = Logger.getLogger(getClass.getName)

  class MyMapper extends Mapper[LongWritable, Text, Text, HMapStIW] {
    val map = new HMapStIW()
    var window = 2

    override def setup(context: Mapper[LongWritable, Text, Text, HMapStIW]#Context) {
      window = context.getConfiguration.getInt("window", 2)
    }

    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, Text, HMapStIW]#Context) = {
      val tokens = tokenize(value)
      for (i <- tokens.indices) {
        map.clear()
        for (j <- Math.max(i - window, 0) until Math.min(i + window + 1, tokens.length)) {
          if (i != j) map.increment(tokens(j))
        }
        context.write(tokens(i), map)
      }
    }
  }

  class MyReducer extends Reducer[Text, HMapStIW, Text, HMapStIW] {
    override def reduce(key: Text, values: java.lang.Iterable[HMapStIW],
                        context: Reducer[Text, HMapStIW, Text, HMapStIW]#Context) = {
      val map = new HMapStIW()
      for (value <- values.asScala) {
        map.plus(value)
      }
      context.write(key, map)
    }
  }

  override def run(argv: Array[String]) : Int = {
    val args = new ConfStripes(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Window: " + args.window())

    val conf = getConf()
    val job = Job.getInstance(conf)

    FileInputFormat.addInputPath(job, new Path(args.input()))
    FileOutputFormat.setOutputPath(job, new Path(args.output()))

    job.setJobName("Cooccurrence Stripes")
    job.setJarByClass(this.getClass)

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[HMapStIW])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[HMapStIW])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, HMapStIW]])

    job.setMapperClass(classOf[MyMapper])
    job.setCombinerClass(classOf[MyReducer])
    job.setReducerClass(classOf[MyReducer])

    job.getConfiguration.setInt("window", args.window())

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
