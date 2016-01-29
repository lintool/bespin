package io.bespin.scala.mapreduce.util

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{Tool, ToolRunner}

/**
  * Base trait for creating entry points for MapReduce jobs
  */
trait BaseConfiguredTool extends Configured with Tool { self =>

  def main(args: Array[String]) {
    ToolRunner.run(self, args)
  }
}
