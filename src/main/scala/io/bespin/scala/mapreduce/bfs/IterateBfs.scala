package io.bespin.scala.mapreduce.bfs

import io.bespin.java.mapreduce.bfs.BfsNode
import io.bespin.scala.mapreduce.util.{BaseConfiguredTool, MapReduceSugar, TypedMapper, TypedReducer}
import org.apache.hadoop.io.IntWritable
import org.rogach.scallop.ScallopConf
import tl.lin.data.array.ArrayListOfIntsWritable
import tl.lin.data.map.HMapII

import scala.collection.JavaConverters._

/**
  * Tool for running one iteration of parallel breadth-first search.
  */
object IterateBfs extends BaseConfiguredTool with MapReduceSugar {
  private val Counter = "Counter"
  private val ReachableInMapper = "ReachableInMapper"
  private val ReachableInReducer = "ReachableInReducer"

  private object MapClass extends TypedMapper[IntWritable, BfsNode, IntWritable, BfsNode] {
    // For buffering distances keyed by destination node.
    private[this] val map = new HMapII

    // For passing along node structure.
    private[this] val intermediateStructure = new BfsNode

    override def setup(context: Context): Unit = {
      // Must clear the map for cases where mapper is
      // re-used (such as for local execution)
      map.clear()
    }

    override def map(nid: IntWritable, node: BfsNode, context: Context): Unit = {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId)
      intermediateStructure.setType(BfsNode.Type.Structure)
      intermediateStructure.setAdjacencyList(node.getAdjacencyList)

      context.write(nid, intermediateStructure)

      if(node.getDistance != Int.MaxValue) {
        context.getCounter(Counter, ReachableInMapper).increment(1)
        // Retain distance to self.
        map.put(nid.get, node.getDistance)

        val adj = node.getAdjacencyList
        val dist = node.getDistance + 1
        // Keep track of distance if it's shorter than previously
        // encountered, or if we haven't encountered this node.
        // Note that putting this into a more scala-idiomatic "foreach" seems
        // to cause it to silently skip nodes in some cases, so a while loop
        // is used instead.
        var i = 0
        while(i < adj.size) {
          val neighbor = adj.get(i)
          if(!map.containsKey(neighbor) || dist < map.get(neighbor)) {
            map.put(neighbor, dist)
          }
          i += 1
        }
      }
    }

    override def cleanup(context: Context): Unit = {
      val k = new IntWritable
      val dist = new BfsNode
      for (e <- map.entrySet.asScala) {
        k.set(e.getKey)
        dist.setNodeId(e.getKey)
        dist.setType(BfsNode.Type.Distance)
        dist.setDistance(e.getValue)
        context.write(k, dist)
      }
    }
  }

  private object ReduceClass extends TypedReducer[IntWritable, BfsNode, IntWritable, BfsNode] {
    private[this] val node = new BfsNode

    override def reduce(nid: IntWritable, iterable: Iterable[BfsNode], context: Context): Unit = {
      val values = iterable.iterator

      var structureReceived: Int = 0
      var dist: Int = Int.MaxValue

      while(values.hasNext) {
        val n = values.next

        if(n.getType == BfsNode.Type.Structure) {
          // This is the structure node; update accordingly
          node.setAdjacencyList(new ArrayListOfIntsWritable(n.getAdjacencyList))
          structureReceived += 1
        } else {
          // This is a message that contains distance
          dist = math.min(dist, n.getDistance)
        }
      }

      node.setType(BfsNode.Type.Complete)
      node.setNodeId(nid.get)
      node.setDistance(dist) // Update the final distance

      if(dist != Int.MaxValue) {
        context.getCounter(Counter, ReachableInReducer).increment(1)
      }

      // Error checking
      if(structureReceived == 1) {
        // Everything checks out, emit final node structures with updated distance.
        context.write(nid, node)
      } else if(structureReceived == 0) {
        // We get into this situation if there exists an edge pointing
        // to a node which has no corresponding node structure (i.e.,
        // distance was passed to a non-existent node)... log but move
        // on.
        log.warn("No structure received for nodeid: " + nid.get)
      } else {
        // This should never happen
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get
          + " struct: " + structureReceived)
      }
    }
  }

  private class Conf(args: Seq[String]) extends ScallopConf(args) {
    lazy val input = opt[String](descr = "input path", required = true)
    lazy val output = opt[String](descr = "output path", required = true)
    lazy val partitions = opt[Int](descr = "number of partitions", required = true)

    mainOptions = Seq(input, output, partitions)
  }

  override def run(argv: Array[String]): Int = {
    val args = new Conf(argv)

    log.info("Tool name: " + this.getClass.getSimpleName)
    log.info(" - input: " + args.input())
    log.info(" - output: " + args.output())
    log.info(" - partitions:" + args.partitions())

    val conf = getConf
    conf.set("mapred.child.java.opts", "-Xmx2048m")

    val thisJob = job(s"IterateBfs[input: ${args.input()}, output: ${args.output()}, " +
      s"partitions: ${args.partitions()}", conf)
      .sequenceFile[IntWritable, BfsNode](args.input())
      .map(MapClass)
      .reduce(ReduceClass, args.partitions())

    time {
      thisJob.saveAsSequenceFile(args.output(), deleteExisting = true)
    }

    0
  }
}
