package io.bespin.scala.util

import org.apache.log4j.Logger

trait TestLogging { self =>
  lazy val log = Logger.getLogger(self.getClass)
}
