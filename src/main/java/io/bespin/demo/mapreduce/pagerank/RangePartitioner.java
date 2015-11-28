package io.bespin.demo.mapreduce.pagerank;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Ranger partitioner. In the context of graph algorithms, ensures that consecutive node ids are
 * blocked together.
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RangePartitioner extends Partitioner<IntWritable, Writable> implements Configurable {
  private int nodeCnt = 0;
  private Configuration conf;

  public RangePartitioner() {}

  @Override
  public int getPartition(IntWritable key, Writable value, int numReduceTasks) {
    return (int) (((float) key.get() / (float) nodeCnt) * numReduceTasks) % numReduceTasks;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    configure();
  }

  private void configure() {
    nodeCnt = conf.getInt("NodeCount", 0);
  }
}
