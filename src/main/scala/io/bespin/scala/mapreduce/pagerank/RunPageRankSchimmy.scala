package io.bespin.scala.mapreduce.pagerank

import java.io.IOException
import java.text.{DecimalFormat, NumberFormat}

import com.google.common.base.Preconditions
import io.bespin.java.mapreduce.pagerank.{NonSplitableSequenceFileInputFormat, PageRankMessages, PageRankNode}
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.{FloatWritable, IntWritable, SequenceFile, Writable}
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.rogach.scallop.ScallopConf
import tl.lin.data.map.HMapIF

import scala.collection.JavaConverters._

/**
  * <p>
  * Main driver program for running the Schimmy implementation of PageRank.
  * </p>
  *
  * <p>
  * The starting and ending iterations will correspond to paths
  * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
  * example, if you specify 0 and 10 as the starting and ending iterations, the
  * driver program will start with the graph structure stored at
  * <code>/base/path/iter0000</code>; final results will be stored at
  * <code>/base/path/iter0010</code>.
  * </p>
  *
  * @see [[RunPageRankBasic]]
  * @see [[io.bespin.java.mapreduce.pagerank.RunPageRankSchimmy]]
  */
object RunPageRankSchimmy extends BaseConfiguredTool with MapReduceSugar {

  private object MapClass
    extends TypedMapper[IntWritable, PageRankNode, IntWritable, FloatWritable] {
    // The neighbor to which we're sending messages.
    private val neighbor = new IntWritable

    // Contents of the messages: partial PageRank mass.
    private val intermediateMass = new FloatWritable

    override def map(nid: IntWritable, node: PageRankNode, context: Context): Unit = {
      var massMessages = 0

      // Distribute PageRank mass to neighbors (along outgoing edges)
      val list = node.getAdjacencyList
      if(list.size > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        val mass = node.getPageRank - StrictMath.log(list.size).toFloat

        // Iterate over neighbors.
        for(i <- 0 until list.size) {
          neighbor.set(list.get(i))
          intermediateMass.set(mass)

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass)
          massMessages += 1
        }
      }

      // Bookkeeping.
      context.getCounter(PageRankMessages.nodes).increment(1)
      context.getCounter(PageRankMessages.massMessages).increment(massMessages)
    }
  }

  // Mapper with in-mapper combiner optimization.
  private object MapWithInMapperCombiningClass
    extends TypedMapper[IntWritable, PageRankNode, IntWritable, FloatWritable] {
    // For buffering PageRank mass contributions keyed by destination node.
    private val map = new HMapIF

    // Clear the buffered values in case this mapper is re-used (as is the case when executing locally).
    override def setup(context: Context): Unit = {
      map.clear()
    }

    override def map(nid: IntWritable, node: PageRankNode, context: Context): Unit = {
      var massMessages = 0
      var massMessagesSaved = 0

      val list = node.getAdjacencyList

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if(list.size > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        val mass = node.getPageRank - StrictMath.log(list.size).toFloat

        // Iterate over neighbors.
        var i: Int = 0
        while (i < list.size) {
          val neighbor: Int = list.get(i)
          if (map.containsKey(neighbor)) {
            // Already message destined for that node; add PageRank mass contribution.
            massMessagesSaved += 1
            map.put(neighbor, sumLogProbs(map.get(neighbor), mass))
          } else {
            // New destination node; add new entry in map.
            massMessages += 1
            map.put(neighbor, mass)
          }
          i += 1
        }
      }

      // Bookkeeping.
      context.getCounter(PageRankMessages.nodes).increment(1)
      context.getCounter(PageRankMessages.massMessages).increment(massMessages)
      context.getCounter(PageRankMessages.massMessagesSaved).increment(massMessagesSaved)
    }

    override def cleanup(context: Context): Unit = {
      // Now emit the messages all at once.
      val k = new IntWritable
      val v = new FloatWritable

      for(e <- map.entrySet.asScala) {
        k.set(e.getKey)
        v.set(e.getValue)
        context.write(k, v)
      }
    }
  }

  // Combiner: sums partial PageRank contributions.
  private object CombineClass
    extends TypedReducer[IntWritable, FloatWritable, IntWritable, FloatWritable] {
    private val intermediateMass = new FloatWritable

    override def reduce(nid: IntWritable, values: Iterable[FloatWritable], context: Context): Unit = {
      // Remember, PageRank mass is stored as a log prob.
      val (massMessages, mass) = values.foldLeft((0, Float.NegativeInfinity)){
        case ((messages, massSum), next) =>
          // Accumulate PageRank mass contributions
          (messages + 1, sumLogProbs(massSum, next))
      }

      // emit aggregated results
      if(massMessages > 0) {
        intermediateMass.set(mass)
        context.write(nid, intermediateMass)
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private object ReduceClass
    extends TypedReducer[IntWritable, FloatWritable, IntWritable, PageRankNode] {
    private var totalMass = Float.NegativeInfinity

    private var reader: SequenceFile.Reader = _

    private val hdfsNid = new IntWritable
    private val hdfsNode = new PageRankNode

    private var hdfsAhead = false

    override def setup(context: Context): Unit = {
      // We're going to open up the file on HDFS that has corresponding node structures.
      // We get the task id and map it to the corresponding part.
      val conf = context.getConfiguration

      val taskId = conf.get("mapred.task.id")
      Preconditions.checkNotNull(taskId)

      // The partition mapping is passed in from the driver.
      val mapping = conf.get("PartitionMapping")
      Preconditions.checkNotNull(mapping)

      val map: Map[Int, String] = mapping.split(";").map(s => {
        val arr = s.split("=")

        log.info(arr(0) + "\t" + arr(1))
        arr(0).toInt -> arr(1)
      }).toMap

      // Get the part number
      val partno = taskId.substring(taskId.length - 7, taskId.length - 2).toInt
      val f = map(partno)

      log.info(s"task id: $taskId")
      log.info(s"partno: $partno")
      log.info(s"file: $f")

      // Try and open the node structures...
      try {
        reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(f)))
      } catch {
        case e: IOException =>
          throw new RuntimeException(s"Couldn't open $f for partno: $partno within: $taskId")
        case t: Throwable => throw t
      }
    }

    override def reduce(nid: IntWritable, values: Iterable[FloatWritable], context: Context): Unit = {
      // The basic algorithm is a merge sort between node structures on HDFS and intermediate
      // key-value pairs coming into this reducer (where the keys are the node ids). Both are
      // sorted, and the reducer is "pushed" intermediate key-value pairs, so the algorithm boils
      // down to properly advancing the node structures file on HDFS.

      // The HDFS node structure file is ahead. This means the incoming node ids don't have
      // corresponding node structure (i.e., messages addressed to non-existent nodes). This may
      // happen if the adjacency lists point to nodes that don't exist. Do nothing.
      if(hdfsNid.get <= nid.get) {

        // We need to advance the HDFS node structure file.
        if(hdfsNid.get < nid.get) {
          if(hdfsAhead) {
            // If we get here, it means that no messages were sent to a particular node in the HDFS
            // node structure file. So we want to emit this node structure.
            hdfsNode.setPageRank(Float.NegativeInfinity)
            context.write(hdfsNid, hdfsNode)
            hdfsAhead = false
          }

          // We're now going to advance the HDFS node structure until we get to the node id of the
          // current message we're processing...
          while(reader.next(hdfsNid, hdfsNode) && hdfsNid.get != nid.get) {
            // If we go past the incoming node id in the HDFS node structure file, then it means that
            // no corresponding no structure exist. That is, a message was sent to a non-existent
            // node: this may happen if adjacency lists point to nodes that don't exist.
            if (hdfsNid.get > nid.get) {
              hdfsAhead = true
              return
            }

            // This is a node that has not messages sent to it... we don't want to node the node
            // structure, so just emit.
            hdfsNode.setPageRank(Float.NegativeInfinity)
            context.write(hdfsNid, hdfsNode)
          }
        }

        // If we get here, it means that the reader ran out of nodes, i.e., next method returned
        // false. This means that the messages were addressed to non-existent nodes.
        if (hdfsNid.get != nid.get) return
      }

      val (massMessagesReceived, mass) = values.foldLeft((0, Float.NegativeInfinity)) {
        case ((messages, massSum), next) => (messages + 1, sumLogProbs(massSum, next))
      }

      totalMass = sumLogProbs(totalMass, mass)

      // Populate the node structure with the updated PageRank value.
      hdfsNode.setPageRank(mass)

      // Emit!
      context.write(nid, hdfsNode)
      context.getCounter(PageRankMessages.massMessagesReceived).increment(massMessagesReceived)

      hdfsAhead = false
    }

    override def cleanup(context: Context): Unit = {
      val conf = context.getConfiguration
      val taskId = conf.get("mapred.task.id")
      val path = conf.get("PageRankMassPath")

      Preconditions.checkNotNull(taskId)
      Preconditions.checkNotNull(path)

      val fs = FileSystem.get(conf)
      val out = fs.create(new Path(s"$path/$taskId"), false)
      out.writeFloat(totalMass)
      out.close()

      // If the HDFS node structure file is ahead, we want to emit the current node structure.
      if(hdfsAhead) {
        hdfsNode.setPageRank(Float.NegativeInfinity)
        context.write(hdfsNid, hdfsNode)
        hdfsAhead = false
      }

      // We have to write out the rest of the nodes we haven't finished reading yet (i.e., these are
      // the ones who don't have any messages sent to them)
      while (reader.next(hdfsNid, hdfsNode)) {
        hdfsNode.setPageRank(Float.NegativeInfinity)
        context.write(hdfsNid, hdfsNode)
      }

      reader.close()
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private object MapPageRankMassDistributionClass
    extends TypedMapper[IntWritable, PageRankNode, IntWritable, PageRankNode] {
    private var missingMass = 0.0f
    private var nodeCnt = 0

    override def setup(context: Context): Unit = {
      val conf = context.getConfiguration

      missingMass = conf.getFloat("MissingMass", 0.0f)
      nodeCnt = conf.getInt("NodeCount", 0)
    }

    override def map(nid: IntWritable, node: PageRankNode, context: Context): Unit = {
      val p = node.getPageRank

      val jump = (Math.log(ALPHA) - Math.log(nodeCnt)).toFloat
      val link = Math.log(1.0 - ALPHA).toFloat +
        sumLogProbs(p, (Math.log(missingMass) - Math.log(nodeCnt)).toFloat)

      node.setPageRank(sumLogProbs(jump, link))
      context.write(nid, node)
    }
  }

  private val ALPHA: Float = 0.15f
  private val FORMAT: NumberFormat = new DecimalFormat("0000")

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val base = opt[String](descr = "base path", required = true)
    lazy val start = opt[Int](descr = "start iteration", required = true)
    lazy val end = opt[Int](descr = "end iteration", required = true)
    lazy val numNodes = opt[Int](descr = "number of nodes", required = true)

    lazy val useCombiner = opt[Boolean](descr = "use combiner", required = false, default = Some(false))
    lazy val useInMapperCombiner = opt[Boolean](descr = "use in-mapper combiner", required = false, default = Some(false))
    lazy val range = opt[Boolean](descr = "use range partitioner", required = false, default = Some(false))

    mainOptions = Seq(base, start, end, numNodes, useCombiner, useInMapperCombiner, range)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Tool name: RunPageRank")
    log.info(" - base path: " + args.base())
    log.info(" - num nodes: " + args.numNodes())
    log.info(" - start iteration: " + args.start())
    log.info(" - end iteration: " + args.end())
    log.info(" - use combiner: " + args.useCombiner())
    log.info(" - use in-mapper combiner: " + args.useInMapperCombiner())
    log.info(" - use range partitioner: " + args.range())

    // Iterate PageRank.
    for(i <- args.start() until args.end()) {
      iteratePageRank(args.base(), i, i + 1, args.numNodes(), args.useCombiner(), args.useInMapperCombiner(), args.range())
    }

    0
  }

  private object ThisRangePartitioner extends RangePartitioner[FloatWritable]

  // Run each iteration.
  private def iteratePageRank(path: String, i: Int, j: Int, n: Int, useCombiner: Boolean,
                              useInMapCombiner: Boolean, useRange: Boolean): Unit = {
    // Each iteration consists of two phases (two MapReduce jobs).
    // Job1: distribute PageRank mass along outgoing edges.
    val mass = phase1(path, i, j, n, useCombiner, useInMapCombiner, useRange)

    // Find out how much PageRank mass got lost at the dangling nodes.
    val missing: Float = Math.max(0.0f, 1.0f - StrictMath.exp(mass).toFloat)

    // Job2: distribute missing mass, take care of random jump factor.
    phase2(path, i, j, n, missing)

  }

  private def phase1(path: String, i: Int, j: Int, n: Int, useCombiner: Boolean,
                     useInMapCombiner: Boolean, useRange: Boolean): Float = {
    val conf = getConf

    val in = path + "/iter" + FORMAT.format(i)
    val out = path + "/iter" + FORMAT.format(j) + "t"
    val outm = out + "-mass"

    // We need to actually count the number of part files to get the number
    // of partitions (because the directory might contain _log).
    val numPartitions = FileSystem.get(conf).listStatus(new Path(in))
      .count(_.getPath.getName.contains("part-"))

    conf.setInt("NodeCount", n)

    val partitioner = if(useRange) {
      val p = new RangePartitioner[PageRankNode]
      p.setConf(conf)
      p
    } else {
      new HashPartitioner[IntWritable, Writable]
    }

    // This is really annoying: the mapping between the partition numbers on
    // disk (i.e., part-XXXX) and what partition the file contains (i.e.,
    // key.hash % #reducer) is arbitrary... so this means that we need to
    // open up each partition, peek inside to find out.
    val key: IntWritable = new IntWritable
    val value: PageRankNode = new PageRankNode
    val status: Array[FileStatus] = FileSystem.get(conf).listStatus(new Path(in))

    val sb: StringBuilder = new StringBuilder
    val paths: String = status.withFilter(_.getPath.getName.contains("part-")).map(f => {
      val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(f.getPath))

      reader.next(key, value)
      val np = partitioner.getPartition(key, value, numPartitions)
      reader.close()

      log.info(f.getPath + "\t" + np)
      np + "=" + f.getPath + ";"
    }).foldLeft("")(_ ++ _)

    log.info(paths.trim)

    log.info("PageRankSchimmy: iteration " + j + ": Phase1")
    log.info(" - input: " + in)
    log.info(" - output: " + out)
    log.info(" - nodeCnt: " + n)
    log.info(" - useCombiner: " + useCombiner)
    log.info(" - useInmapCombiner: " + useInMapCombiner)
    log.info(" - numPartitions: " + numPartitions)
    log.info(" - useRange: " + useRange)
    log.info("computed number of partitions: " + numPartitions)

    val numReduceTasks = numPartitions

    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024)
    //conf.set("mapred.child.java.opts", "-Xmx2048m");

    conf.set("PageRankMassPath", outm)
    conf.set("BasePath", in)
    conf.set("PartitionMapping", paths.trim)

    conf.setBoolean("mapred.map.tasks.speculative.execution", false)
    conf.setBoolean("mapred.reduce.tasks.speculative.execution", false)

    val thisJob = job(s"PageRankSchimmy:iteration$j:Phase1", conf)
      .sequenceFile[IntWritable, PageRankNode](in)

    val mappedJob = thisJob.map(if(useInMapCombiner) MapWithInMapperCombiningClass else MapClass)
    val partitionedJob = if(useRange) mappedJob.partition(ThisRangePartitioner) else mappedJob
    val reducedJob = {if(useCombiner) partitionedJob.combine(CombineClass) else partitionedJob}
      .reduce(ReduceClass, numReduceTasks)

    FileSystem.get(conf).delete(outm, true)

    time {
      reducedJob.saveAsSequenceFile(out, deleteExisting = true)
    }

    val fs = FileSystem.get(getConf)
    val mass = fs.listStatus(new Path(outm)).foldLeft(Float.NegativeInfinity)(
      (m, f) => {
        val fin = fs.open(f.getPath)
        val nextMass = sumLogProbs(m, fin.readFloat)
        fin.close()
        nextMass
      })

    mass
  }

  private def phase2(path: String, i: Int, j: Int, n: Int, missing: Float): Unit = {
    val in = s"$path/iter" + FORMAT.format(j) + "t"
    val out = s"$path/iter" + FORMAT.format(j)

    log.info(s"PageRank: iteration $j: Phase2")
    log.info(s" - input: $in")
    log.info(s" - output: $out")

    val jobConf = getConf
    jobConf.setBoolean("mapred.map.tasks.speculative.execution", false)
    jobConf.setBoolean("mapred.reduce.tasks.speculative.execution", false)
    jobConf.setFloat("MissingMass", missing)
    jobConf.setInt("NodeCount", n)

    val thisJob =
      job(s"PageRank:Basic:iteration$j:Phase2", jobConf)
        .file(new Path(in), classOf[NonSplitableSequenceFileInputFormat[IntWritable, PageRankNode]])
        .map(MapPageRankMassDistributionClass)
        .reduce(0)

    time {
      thisJob.saveAsSequenceFile(new Path(out), deleteExisting = true)
    }
  }

  private def sumLogProbs(a: Float, b: Float): Float = {
    if (a == Float.NegativeInfinity)
      b
    else if (b == Float.NegativeInfinity)
      a
    else if (a < b) {
      (b + StrictMath.log1p(StrictMath.exp(a - b))).toFloat
    } else {
      (a + StrictMath.log1p(StrictMath.exp(b - a))).toFloat
    }
  }
}
