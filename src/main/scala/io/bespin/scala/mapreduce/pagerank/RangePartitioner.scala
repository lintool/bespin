package io.bespin.scala.mapreduce.pagerank

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.io.{IntWritable, Writable}
import org.apache.hadoop.mapreduce.Partitioner


class RangePartitioner[T <: Writable] extends Partitioner[IntWritable, T] with Configurable {
  private var nodeCnt: Int = 0
  private var conf: Configuration = null

  def getPartition(key: IntWritable, value: T, numReduceTasks: Int): Int = {
    ((key.get.toFloat / nodeCnt.toFloat) * numReduceTasks).toInt % numReduceTasks
  }

  def getConf: Configuration = {
    conf
  }

  def setConf(conf: Configuration): Unit = {
    this.conf = conf
    configure()
  }

  private def configure(): Unit = {
    nodeCnt = conf.getInt("NodeCount", 0)
  }
}
