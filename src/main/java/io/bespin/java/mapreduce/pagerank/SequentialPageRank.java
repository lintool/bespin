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

package io.bespin.java.mapreduce.pagerank;

import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.importance.Ranking;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * <p>
 * Program that computes PageRank for a graph using the <a
 * href="https://github.com/jrtom/jung">JUNG</a> package. Program takes two command-line
 * arguments: the first is a file containing the graph data, and the second is the random jump
 * factor (a typical setting is 0.15).
 * </p>
 *
 * <p>
 * The graph should be represented as an adjacency list. Each line should have at least one token;
 * tokens should be tab delimited. The first token represents the unique id of the source node;
 * subsequent tokens represent its link targets (i.e., outlinks from the source node). For
 * completeness, there should be a line representing all nodes, even nodes without outlinks (those
 * lines will simply contain one token, the source node id).
 * </p>
 *
 * @author Jimmy Lin
 */
public class SequentialPageRank {
  private SequentialPageRank() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-jump", metaVar = "[num]", usage = "random jump factor")
    float alpha = 0.15f;
  }

  public static void main(String[] argv) throws IOException {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }

    int edgeCnt = 0;
    DirectedSparseGraph<String, Integer> graph = new DirectedSparseGraph<>();

    BufferedReader data =
        new BufferedReader(new InputStreamReader(new FileInputStream(args.input)));

    String line;
    while ((line = data.readLine()) != null) {
      String[] arr = line.split("\\t");

      for (int i = 1; i < arr.length; i++) {
        graph.addEdge(edgeCnt++, arr[0], arr[i]);
      }
    }

    data.close();

    WeakComponentClusterer<String, Integer> clusterer = new WeakComponentClusterer<>();
    Set<Set<String>> components = clusterer.apply(graph);

    System.out.println("Number of components: " + components.size());
    System.out.println("Number of edges: " + graph.getEdgeCount());
    System.out.println("Number of nodes: " + graph.getVertexCount());
    System.out.println("Random jump factor: " + args.alpha);

    // Compute PageRank.
    PageRank<String, Integer> ranker = new PageRank<>(graph, args.alpha);
    ranker.evaluate();

    // Use priority queue to sort vertices by PageRank values.
    PriorityQueue<Ranking<String>> q = new PriorityQueue<>();
    int i = 0;
    for (String pmid : graph.getVertices()) {
      q.add(new Ranking<>(i++, ranker.getVertexScore(pmid), pmid));
    }

    // Print PageRank values.
    System.out.println("\nPageRank of nodes, in descending order:");
    Ranking<String> r;
    while ((r = q.poll()) != null) {
      System.out.println(String.format("%s\t%.5f ", r.getRanked(), Math.log(r.rankScore)));
    }
  }
}
