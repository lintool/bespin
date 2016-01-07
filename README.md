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

The datasets are stored in the [Bespin data repo](https://github.com/lintool/bespin-data).

+ The file `Shakespeare.txt` contains the [The Complete Works of William Shakespeare](http://www.gutenberg.org/ebooks/100) from [Project Gutenberg](http://www.gutenberg.org/).
+ The file `p2p-Gnutella08-adj.txt` contains a [snapshot of the Gnutella peer-to-peer file sharing network from August 2002](http://snap.stanford.edu/data/p2p-Gnutella08.html), where nodes represent hosts in the Gnutella network topology and edges represent connections between the Gnutella hosts. This dataset is available from the [Stanford Network Analysis Project](http://snap.stanford.edu/).


## Word Count in MapReduce and Spark

Make sure you've downloaded the Shakespeare collection (see "Getting Started" above). Running word count in Java MapReduce:

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
$ hadoop fs -cat wc-jmr-combiner/part-r-0000* | awk '{print $1,$2;}' | sort > counts.jmr.combiner.txt
$ hadoop fs -cat wc-smr-combiner/part-r-0000* | awk '{print $1,$2;}' | sort > counts.smr.combiner.txt
$ hadoop fs -cat wc-spark-default/part-0000* | sed -E 's/^\((.*),([0-9]+)\)$/\1 \2/' | sort > counts.spark.default.txt
$ diff counts.jmr.combiner.txt counts.smr.combiner.txt
$ diff counts.jmr.combiner.txt counts.spark.default.txt
```

**Tip:** We use `awk` in some cases and `sed` in others because `sed` does not accept control characters such as `\t` (but GNU `sed` does), so you have to insert a literal tab in the command line. This is awkward: on Mac OS X, you need to type `^V^I`; so it's just easier with `awk`.

## Computing Bigram Relative Frequencies in MapReduce

Make sure you've downloaded the Shakespeare collection (see "Getting Started" above). Running a simple bigram count:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bigram.BigramCount \
   -input data/Shakespeare.txt -output bigram-count
```

Computing bigram relative frequencies using the "pairs" implementation:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bigram.ComputeBigramRelativeFrequencyPairs \
   -input data/Shakespeare.txt -output bigram-freq-mr-pairs -textOutput
```

Computing bigram relative frequencies using the "stripes" implementation:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bigram.ComputeBigramRelativeFrequencyStripes \
   -input data/Shakespeare.txt -output bigram-freq-mr-stripes -textOutput
```

To obtain human-readable output, make sure to use the `-textOutput` option; otherwise, the job defaults to `SequenceFile` output.

Let's spot-check the output to make sure the results are correct. For example, what are the bigrams that begin with "dream"?

```
$ hadoop fs -cat bigram-count/part* | grep '^dream '
dream again	2
dream and	2
dream are	1
dream as	1
dream away	2
...
```

What is the sum of all these counts?

```
$ hadoop fs -cat bigram-count/part* | grep '^dream ' | cut -f 2 | awk '{sum+=$1} END {print sum}'
79
```

Confirm that the numbers match the "pairs" implementation of the relative frequency computations:

```
$ hadoop fs -cat bigram-freq-pairs/part* | grep '(dream, '
```

And the "stripes" implementation of the relative frequency computations:

```
$ hadoop fs -cat bigram-freq-stripes/part* | awk '/^dream\t/'
```

**Tip:** Note that `grep` in Mac OS X accepts `\t`, but not on Linux; strictly speaking, `grep` uses regular expressions as defined by POSIX, and for whatever reasons POSIX does not define `\t` as tab. One workaround is to use `-P`, which specifies Perl regular expressions; however the `-P` option does not exist in Mac OS X.

Here's how you can verify that the pairs and stripes implementation give you the same results:

```
$ cat bigram-freq-mr-pairs/part-r-0000* | awk '{print $1$2,$3;}' | grep -v ",\*)" | sort > freq.mr.pairs.txt
$ cat bigram-freq-mr-stripes/part-r-0000* | perl -ne '%H=();m/([^\t]+)\t\{(.*)\}/; $k=$1; @k=split ", ",$2; foreach(@k){@p=split "=",$_;$H{$p[0]}=$p[1];}; foreach (sort keys %H) {print "($k,$_) $H{$_}\n";}' | sort > freq.mr.stripes.txt
$ diff freq.mr.stripes.txt freq.mr.pairs.txt
```

## Computing Term Co-occurrence Matrix in MapReduce

Make sure you've downloaded the Shakespeare collection (see "Getting Started" above). Running the "pairs" implementation:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixPairs \
   -input data/Shakespeare.txt -output cooccur-pairs -window 2
```

Running the "stripes" implementation:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixStripes \
   -input data/Shakespeare.txt -output cooccur-stripes -window 2
```

Let's spot check the results. For example, here are all the terms the co-occur with "dream" with the "pairs" implementation:

```
$ hadoop fs -cat cooccur-pairs/part* | grep '(dream, '
```

We can verify that the "stripes" implementation gives the same results.

```
$ hadoop fs -cat cooccur-stripes/part* | awk '/^dream\t/'
```

**Tip:** Note that `grep` in Mac OS X accepts `\t`, but not on Linux; strictly speaking, `grep` uses regular expressions as defined by POSIX, and for whatever reasons POSIX does not define `\t` as tab. One workaround is to use `-P`, which specifies Perl regular expressions; however the `-P` option does not exist in Mac OS X.

## Inverted Indexing and Boolean Retrieval in MapReduce

Make sure you've downloaded the Shakespeare collection (see "Getting Started" above). Building the inverted index:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.search.BuildInvertedIndex \
   -input data/Shakespeare.txt -output index
```

Looking up an individual postings list:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.search.LookupPostings \
   -index index -collection data/Shakespeare.txt -term "star-cross'd"
```

Running a boolean retrieval:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.search.BooleanRetrieval \
   -index index -collection data/Shakespeare.txt -query "white red OR rose AND pluck AND"
```

Note that the query must be in [Reverse Polish notation](https://en.wikipedia.org/wiki/Reverse_Polish_notation), so the above is equivalent to `(white OR red) AND rose AND pluck` in standard infix notation.

## Parallel Breadth-First Search in MapReduce

Make sure you've grabbed the sample graph data (see "Getting Started" above). First, convert the plain-text adjacency list representation into Hadoop `Writable` records:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.EncodeBfsGraph \
   -input data/p2p-Gnutella08-adj.txt -output graph-BFS/iter0000 -src 367
```

In the current implementation, you have to run a MapReduce job for every iteration:

```
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0000 -output graph-BFS/iter0001 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0001 -output graph-BFS/iter0002 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0002 -output graph-BFS/iter0003 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0003 -output graph-BFS/iter0004 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0004 -output graph-BFS/iter0005 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0005 -output graph-BFS/iter0006 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0006 -output graph-BFS/iter0007 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0007 -output graph-BFS/iter0008 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0008 -output graph-BFS/iter0009 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0009 -output graph-BFS/iter0010 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0010 -output graph-BFS/iter0011 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0011 -output graph-BFS/iter0012 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0012 -output graph-BFS/iter0013 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0013 -output graph-BFS/iter0014 -partitions 5
hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input graph-BFS/iter0014 -output graph-BFS/iter0015 -partitions 5
```

Here's a bash script to iterate through the above MapReduce jobs:

```
#!/bin/bash
for i in `seq 0 14`;
do
    echo "Iteration $i: reading graph-BFS/iter000$i, writing: graph-BFS/iter000$(($i+1))"
    hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.IterateBfs -input "graph-BFS/iter000$i" -output "graph-BFS/iter000$(($i+1))" -partitions 5
done
```

The MapReduce job counters tell you how many nodes are reachable at each iteration:

Iteration |  Reachable
---|---:
0  |    1
1  |    9
2  |   65
3  |  257
4  |  808
5  | 1934
6  | 3479
7  | 4790
8  | 5444
9  | 5797
10 | 5920
11 | 5990
12 | 6018
13 | 6026
14 | 6028
15 | 6028

To find all the nodes that are reachable at a particular iteration, run the following job:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.FindReachableNodes \
   -input graph-BFS/iter0005 -output graph-BFS/reachable-iter0005

$ hadoop fs -cat 'graph-BFS/reachable-iter0005/part*' | wc
```

To find all the nodes that are at a particular distance (e.g., the search frontier), run the following job:

```
$ hadoop jar target/bespin-0.1.0-SNAPSHOT.jar io.bespin.java.mapreduce.bfs.FindNodeAtDistance \
   -input graph-BFS/iter0005 -output graph-BFS/d5 -distance 5

$ hadoop fs -cat 'graph-BFS/d5/part*' | wc
```

## PageRank in MapReduce

Make sure you've grabbed the sample graph data (see "Getting Started" above). First, convert the plain-text adjacency list representation into Hadoop `Writable` records:

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

$ hadoop fs -cat graph-PageRank-top20/part-r-00000
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

