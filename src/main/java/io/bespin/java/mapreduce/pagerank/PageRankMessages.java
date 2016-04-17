package io.bespin.java.mapreduce.pagerank;

/**
 * Publicly accessible enum of message types shared across PageRank implementations
 */
public enum PageRankMessages {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
}
