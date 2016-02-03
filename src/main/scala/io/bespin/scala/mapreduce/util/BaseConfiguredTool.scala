package io.bespin.scala.mapreduce.util

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{ToolRunner, Tool}
import org.apache.log4j._

/**
  * Base trait for creating entry points for MapReduce jobs
  */
trait BaseConfiguredTool extends Configured with Tool { self =>
  protected lazy val log = Logger.getLogger(self.getClass)

  /**
    * Function which logs the running time of any function in seconds.
    * Note that this only measures the rough amount of time it takes for "f" to return, and is not suitable for benchmarks
    * which require precise measurements.
    */
  def time(f: => Unit): Unit = {
    val startTime: Long = System.currentTimeMillis()
    f
    log.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds")
  }

  def main(args: Array[String]) {
    ToolRunner.run(self, args)
  }
}
