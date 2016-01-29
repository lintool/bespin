package io.bespin.scala.util

import org.apache.hadoop.conf.Configured
import org.apache.hadoop.util.{Tool, ToolRunner}

/**
  * Base trait for creating entry points for MapReduce jobs
  */
trait BaseConfiguredRunnable extends Configured with Tool { self =>

  def main(args: Array[String]) {
    ToolRunner.run(self, args)
  }
}
