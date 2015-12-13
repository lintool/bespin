# Bespin

Bespin is a library that contains reference implementations of "big data" algorithms in MapReduce and Spark.

## Getting Started

Build the package:

```
$ mvn clean package
```

Grab the data:

```
$ mkdir data
$ curl http://lintool.github.io/bespin-data/Shakespeare.txt > data/Shakespeare.txt
$ curl http://lintool.github.io/bespin-data/p2p-Gnutella08-adj.txt > data/p2p-Gnutella08-adj.txt
```

## Word Count in MapReduce and Spark

Running word count in Java MapReduce:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.wordcount.WordCount \
   -input data/Shakespeare.txt -output wc-jmr-combiner
```

To enable the "in-mapper combining" optimization, use the `-imc` option.

Running word count in Scala MapReduce:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.scala.mapreduce.wordcount.WordCount \
   --input data/Shakespeare.txt --output wc-smr-combiner
```

To enable the "in-mapper combining" optimization, use the `--imc` option.

And finally, running word count in Spark:

```
$ spark-submit --class io.bespin.scala.spark.wordcount.WordCount target/bespin-0.1.0-SNAPSHOT.jar \
    --input data/Shakespeare.txt --output wc-spark-default
```

To enable the "in-mapper combining" optimization in Spark, use the `--imc` option (although this optimization doesn't do anything: exercise left to the reader as to why).

Compare results to make sure they are the same:

```
$ cat wc-jmr-combiner/part-r-0000* | sed -E 's/^V^I/ /' > counts.jmr.combiner.txt
$ cat wc-smr-combiner/part-r-0000* | sed -E 's/^V^I/ /' > counts.smr.combiner.txt
$ cat wc-spark-default/part-0000* | sed -E 's/^\((.*),([0-9]+)\)$/\1 \2/' | sort > counts.spark.default.txt
$ diff counts.jmr.combiner.txt counts.smr.combiner.txt
$ diff counts.jmr.combiner.txt counts.spark.default.txt
```

**Tip:** `sed` does not accept control characters such as `\t`, so you have to insert a literal tab in the command line. To do so on Mac OS X, type `^V^I`.

## PageRank in MapReduce

Make sure you've grabbed the sample graph data; see "Getting Started" above. First, convert the plain-text adjacency list representation into Hadoop `Writable` records:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.pagerank.BuildPageRankRecords \
   -input data/p2p-Gnutella08-adj.txt -output graph-PageRankRecords -numNodes 6301
```

Create the directory where the graph is going to go:

```
$ hadoop fs -mkdir graph-PageRank
```

Partition the graph:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.pagerank.PartitionGraph \
   -input graph-PageRankRecords -output graph-PageRank/iter0000 -numPartitions 5 -numNodes 6301
```

Run 10 iterations:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.pagerank.RunPageRankBasic \
   -base graph-PageRank -numNodes 6301 -start 0 -end 10 -useCombiner
```

Extract the top 20 nodes by PageRank value and examine the results:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.pagerank.FindMaxPageRankNodes \
   -input graph-PageRank/iter0010 -output graph-PageRank-top20 -top 20

$ cat graph-PageRank-top20/part-r-00000
367     -6.0373473
249     -6.1263785
145     -6.1874337
264     -6.2151237
266     -6.2329764
123     -6.2852564
127     -6.2868505
122     -6.29074
1317    -6.2959766
5       -6.3027534
251     -6.329841
427     -6.3382115
149     -6.4021697
176     -6.4235106
353     -6.4398885
390     -6.44405
559     -6.4549174
124     -6.4570518
4       -6.470556
7       -6.501463
```

Compare the results with a sequential PageRank implementation:

```
$ mvn exec:java -Dexec.mainClass=io.bespin.java.mapreduce.pagerank.SequentialPageRank \
   -Dexec.args="-input data/p2p-Gnutella08-adj.txt -jump 0.15"
```

