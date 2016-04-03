package io.bespin.scala.mapreduce.pagerank

import java.text.{DecimalFormat, NumberFormat}

import com.google.common.base.Preconditions
import io.bespin.java.mapreduce.pagerank.{NonSplitableSequenceFileInputFormat, PageRankMessages, PageRankNode}
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IntWritable
import org.rogach.scallop.ScallopConf
import tl.lin.data.map.HMapIF

import scala.collection.JavaConverters._

/**
  * <p>
  * Main driver program for running the basic (non-Schimmy) implementation of
  * PageRank.
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
  * @see RunPageRankSchimmy
  * @author Jimmy Lin
  * @author Michael Schatz
  */
object RunPageRankBasic extends BaseConfiguredTool with MapReduceSugar {

  private object MapClass extends TypedMapper[IntWritable, PageRankNode, IntWritable, PageRankNode] {

    // The neighbor to which we're sending messages.
    private val neighbor = new IntWritable

    // Contents of the messages: partial PageRank mass.
    private val intermediateMass = new PageRankNode

    // For passing along node structure.
    private val intermediateStructure = new PageRankNode

    override def map(nid: IntWritable, node: PageRankNode, context: Context): Unit = {
      // Pass along node structure
      intermediateStructure.setNodeId(node.getNodeId)
      intermediateStructure.setType(PageRankNode.Type.Structure)
      intermediateStructure.setAdjacencyList(node.getAdjacencyList)

      context.write(nid, intermediateStructure)

      val massMessages = node.getAdjacencyList.size

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if(massMessages > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        val list = node.getAdjacencyList
        val mass = node.getPageRank - StrictMath.log(list.size()).toFloat

        context.getCounter(PageRankMessages.edges).increment(list.size)

        list.iterator.asScala.foreach { nextNeighbor =>
          neighbor.set(nextNeighbor)
          intermediateMass.setNodeId(nextNeighbor)
          intermediateMass.setType(PageRankNode.Type.Mass)
          intermediateMass.setPageRank(mass)

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass)
        }
      }

      // Bookkeeping.
      context.getCounter(PageRankMessages.nodes).increment(1)
      context.getCounter(PageRankMessages.massMessages).increment(massMessages)
    }
  }

  private object MapWithInMapperCombiningClass extends TypedMapper[IntWritable, PageRankNode, IntWritable, PageRankNode] {
    // For buffering PageRank mass contributions keyed by destination node
    private val map = new HMapIF

    // For passing along node structure.
    private val intermediateStructure = new PageRankNode

    override def setup(context: Context): Unit = {
      // When running in local mode, the mappers are re-used and must be re-set. This is not
      // necessarily required in distributed mode.
      map.clear()
    }

    override def map(nid: IntWritable, node: PageRankNode, context: Context): Unit = {
      // Pass along node structure
      intermediateStructure.setNodeId(node.getNodeId)
      intermediateStructure.setType(PageRankNode.Type.Structure)
      intermediateStructure.setAdjacencyList(node.getAdjacencyList)

      context.write(nid, intermediateStructure)

      // Variables for bookkeeping
      var massMessages = 0
      var massMessagesSaved = 0

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if(node.getAdjacencyList.size > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        val list = node.getAdjacencyList
        val mass = node.getPageRank - StrictMath.log(list.size()).toFloat

        context.getCounter(PageRankMessages.edges).increment(list.size())

        list.iterator().asScala.foreach { nextNeighbor =>
          if(map.containsKey(nextNeighbor)) {
            // Already message destined for that node; add PageRank mass contribution.
            massMessagesSaved += 1
            map.put(nextNeighbor, sumLogProbs(map.get(nextNeighbor), mass))
          } else {
            // New destination node; add new entry in map.
            massMessages += 1
            map.put(nextNeighbor, mass)
          }
        }
      }
      // Bookkeeping.
      context.getCounter(PageRankMessages.nodes).increment(1)
      context.getCounter(PageRankMessages.massMessages).increment(massMessages)
      context.getCounter(PageRankMessages.massMessagesSaved).increment(massMessagesSaved)
    }

    override def cleanup(context: Context): Unit = {
      // Now emit all messages for this node at once.
      val k = new IntWritable
      val mass = new PageRankNode

      map.entrySet.iterator().asScala.foreach { e =>
        k.set(e.getKey)

        mass.setNodeId(e.getKey)
        mass.setType(PageRankNode.Type.Mass)
        mass.setPageRank(e.getValue)

        context.write(k, mass)
      }
    }
  }

  private object CombineClass extends TypedReducer[IntWritable, PageRankNode, IntWritable, PageRankNode] {
    private val intermediateMass = new PageRankNode

    override def reduce(nid: IntWritable, values: Iterable[PageRankNode], context: Context): Unit = {

      var massMessages = 0
      val mass = values.foldLeft(Float.NegativeInfinity)((mass, n) =>
        if(n.getType == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n)
          mass
        } else {
          // Accumulate PageRank mass contributions
          massMessages += 1
          sumLogProbs(mass, n.getPageRank)
        })

      // Emit aggregated results
      if(massMessages > 0) {
        intermediateMass.setNodeId(nid.get())
        intermediateMass.setType(PageRankNode.Type.Mass)
        intermediateMass.setPageRank(mass)

        context.write(nid, intermediateMass)
      }
    }
  }

  private object ReduceClass extends
    TypedReducer[IntWritable, PageRankNode, IntWritable, PageRankNode] {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    private var totalMass: Float = Float.NegativeInfinity

    override def reduce(nid: IntWritable, iterable: Iterable[PageRankNode], context: Context): Unit = {
      val values = iterable.iterator

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      val node = new PageRankNode

      node.setType(PageRankNode.Type.Complete)
      node.setNodeId(nid.get)

      // Bookkeeping counters
      var massMessagesReceived = 0
      var structureReceived = 0

      val accumulatedMass = values.foldLeft(Float.NegativeInfinity)((mass, n) =>
        if (n.getType == PageRankNode.Type.Structure) {
          // This is the structure; update accordingly.
          structureReceived += 1
          node.setAdjacencyList(n.getAdjacencyList)
          mass
        } else {
          // This is a message that contains PageRank mass; accumulate.
          massMessagesReceived += 1
          sumLogProbs(mass, n.getPageRank)
        })

      // Update the final accumulated PageRank mass.
      node.setPageRank(accumulatedMass)
      // Bookkeeping
      context.getCounter(PageRankMessages.massMessagesReceived).increment(massMessagesReceived)

      // Error checking
      if(structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node)

        // Keep track of total PageRank mass
        totalMass = sumLogProbs(totalMass, accumulatedMass)
      } else if(structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRankMessages.missingStructure).increment(1)
        log.warn("No structure received for nodeid: " + nid.get + " mass: " + massMessagesReceived)
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException(s"Multiple structure received for nodeid: ${nid.get} " +
          s"mass: $massMessagesReceived struct: $structureReceived")
      }
    }

    override def cleanup(context: Context): Unit = {
      val conf = context.getConfiguration
      val taskId = conf.get("mapred.task.id")
      val path = conf.get("PageRankMassPath")

      Preconditions.checkNotNull(taskId)
      Preconditions.checkNotNull(path)

      // Write to a file the amount of PageRank mass we've seen in this reducer
      val fs: FileSystem = FileSystem.get(context.getConfiguration)
      val out: FSDataOutputStream = fs.create(new Path(s"$path/$taskId"), false)
      out.writeFloat(totalMass)
      out.close()
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private object MapPageRankMassDistributionClass extends TypedMapper[IntWritable, PageRankNode, IntWritable, PageRankNode] {
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
      val link: Float = Math.log(1.0f - ALPHA).toFloat +
        sumLogProbs(p, (Math.log(missingMass) - Math.log(nodeCnt)).toFloat)

      node.setPageRank(sumLogProbs(jump, link))
      context.write(nid, node)
    }

  }

  private val ALPHA: Float = 0.15f
  private val formatter: NumberFormat = new DecimalFormat("0000")

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
    (args.start() until args.end()).foreach { i =>
      iteratePageRank(i, i + 1, args.base(), args.numNodes(), args.useCombiner(), args.useInMapperCombiner())
    }

    0
  }

  private def iteratePageRank(i: Int, j: Int, basePath: String, numNodes: Int,
                              useCombiner: Boolean, useInMapperCombiner: Boolean): Unit = {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges
    val mass = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner)

    // Find out how much PageRank mass got lost at the dangling nodes
    val missing = 1f - StrictMath.exp(mass).toFloat

    // Job 2: distribute missing mass, take care of random jump factor
    phase2(i, j, missing, basePath, numNodes)
  }

  private def phase1(i: Int, j: Int, basePath: String, numNodes: Int,
                     useCombiner: Boolean, useInMapperCombiner: Boolean): Float = {

    val in = s"$basePath/iter" + formatter.format(i)
    val out = s"$basePath/iter" + formatter.format(j) + "t"
    val outm = s"$out-mass"


    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    val numPartitions: Int = FileSystem.get(getConf)
      .listStatus(new Path(in)).count(_.getPath.getName.contains("part-"))

    log.info(s"PageRank: iteration $j: Phase1")
    log.info(s" - input: $in")
    log.info(s" - output: $out")
    log.info(s" - nodeCnt: $numNodes")
    log.info(s" - useCombiner: $useCombiner")
    log.info(s" - useInMapCombiner: $useInMapperCombiner")
    log.info(s"computed number of partitions: $numPartitions")

    val jobConf = getConf
    jobConf.setInt("NodeCount", numNodes)
    jobConf.setBoolean("mapred.map.tasks.speculative.execution", false)
    jobConf.setBoolean("mapred.reduce.tasks.speculative.execution", false)
    jobConf.set("PageRankMassPath", outm)

    val numReducers = numPartitions

    val mapJob =
      job(s"PageRank:Basic:iteration$j:Phase1", jobConf)
        .file(new Path(in), classOf[NonSplitableSequenceFileInputFormat[IntWritable, PageRankNode]])
        .map(if(useInMapperCombiner) MapWithInMapperCombiningClass else MapClass)

    val reduceJob = { if(useCombiner) mapJob.combine(CombineClass) else mapJob }
      .reduce(ReduceClass, numReducers)

    FileSystem.get(jobConf).delete(new Path(outm), true)

    time {
      reduceJob.saveAsSequenceFile(new Path(out), deleteExisting = true)
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

  private def phase2(i: Int, j: Int, missing: Float, basePath: String, numNodes: Int): Unit = {
    val in = s"$basePath/iter" + formatter.format(j) + "t"
    val out = s"$basePath/iter" + formatter.format(j)

    log.info(s"PageRank: iteration $j: Phase2")
    log.info(s" - input: $in")
    log.info(s" - output: $out")

    val jobConf = getConf
    jobConf.setBoolean("mapred.map.tasks.speculative.execution", false)
    jobConf.setBoolean("mapred.reduce.tasks.speculative.execution", false)
    jobConf.setFloat("MissingMass", missing)
    jobConf.setInt("NodeCount", numNodes)

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
