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
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.wordcount.WordCount \
   -input data/Shakespeare.txt -output wc-jmr-combiner
```

To enable the "in-mapper combining" optimization, use the `-imc` option.

Running word count in Scala MapReduce:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.scala.mapreduce.wordcount.WordCount \
   --input data/Shakespeare.txt --output wc-smr-combiner
```

To enable the "in-mapper combining" optimization, use the `--imc` option.

And finally, running word count in Spark:

```
$ spark-submit --class io.bespin.scala.spark.wordcount.WordCount target/bespin-1.1.0-SNAPSHOT-fatjar.jar \
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
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bigram.BigramCount \
   -input data/Shakespeare.txt -output bigram-count
```

Computing bigram relative frequencies using the "pairs" implementation:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bigram.ComputeBigramRelativeFrequencyPairs \
   -input data/Shakespeare.txt -output bigram-freq-mr-pairs -textOutput
```

Computing bigram relative frequencies using the "stripes" implementation:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bigram.ComputeBigramRelativeFrequencyStripes \
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
$ hadoop fs -cat bigram-freq-mr-pairs/part* | grep '(dream, '
```

And the "stripes" implementation of the relative frequency computations:

```
$ hadoop fs -cat bigram-freq-mr-stripes/part* | awk '/^dream\t/'
```

**Tip:** Note that `grep` in Mac OS X accepts `\t`, but not on Linux; strictly speaking, `grep` uses regular expressions as defined by POSIX, and for whatever reasons POSIX does not define `\t` as tab. One workaround is to use `-P`, which specifies Perl regular expressions; however the `-P` option does not exist in Mac OS X.

Here's how you can verify that the pairs and stripes implementation give you the same results:

```
$ hadoop fs -cat bigram-freq-mr-pairs/part-r-0000* | awk '{print $1$2,$3;}' | grep -v ",\*)" | sort > freq.mr.pairs.txt
$ hadoop fs -cat bigram-freq-mr-stripes/part-r-0000* | perl -ne '%H=();m/([^\t]+)\t\{(.*)\}/; $k=$1; @k=split ", ",$2; foreach(@k){@p=split "=",$_;$H{$p[0]}=$p[1];}; foreach (sort keys %H) {print "($k,$_) $H{$_}\n";}' | sort > freq.mr.stripes.txt
$ diff freq.mr.stripes.txt freq.mr.pairs.txt
```

## Computing Term Co-occurrence Matrix in MapReduce

Make sure you've downloaded the Shakespeare collection (see "Getting Started" above). Running the "pairs" implementation:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixPairs \
   -input data/Shakespeare.txt -output cooccur-pairs -window 2
```

Running the "stripes" implementation:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.cooccur.ComputeCooccurrenceMatrixStripes \
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
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.search.BuildInvertedIndex \
   -input data/Shakespeare.txt -output index
```

Looking up an individual postings list:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.search.LookupPostings \
   -index index -collection data/Shakespeare.txt -term "star-cross'd"
```

Running a boolean retrieval:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.search.BooleanRetrieval \
   -index index -collection data/Shakespeare.txt -query "white red OR rose AND pluck AND"
```

Note that the query must be in [Reverse Polish notation](https://en.wikipedia.org/wiki/Reverse_Polish_notation), so the above is equivalent to `(white OR red) AND rose AND pluck` in standard infix notation.

## Parallel Breadth-First Search in MapReduce

Make sure you've grabbed the sample graph data (see "Getting Started" above). First, convert the plain-text adjacency list representation into Hadoop `Writable` records:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.EncodeBfsGraph \
   -input data/p2p-Gnutella08-adj.txt -output graph-BFS/iter0000 -src 367
```

In the current implementation, you have to run a MapReduce job for every iteration, like this:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.IterateBfs \
   -input graph-BFS/iter0000 -output graph-BFS/iter0001 -partitions 5
```

Here's a bash script to run a bunch of iterations:

```
#!/bin/bash

for i in `seq 0 14`; do
  cur=`echo $i | awk '{printf "%04d\n", $0;}'`
  next=`echo $(($i+1)) | awk '{printf "%04d\n", $0;}'`
  echo "Iteration $i: reading graph-BFS/iter$cur, writing: graph-BFS/iter$next"
  hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.IterateBfs -input "graph-BFS/iter$cur" -output "graph-BFS/iter$next" -partitions 5
done
```

The MapReduce job counters tell you how many nodes are reachable at each iteration:

|Iteration |  Reachable | Distance
|---------:|-----------:|--------:
|        0 |          1 |       1
|        1 |          9 |       8
|        2 |         65 |      56
|        3 |        257 |     192
|        4 |        808 |     551
|        5 |       1934 |    1126
|        6 |       3479 |    1545
|        7 |       4790 |    1311
|        8 |       5444 |     654
|        9 |       5797 |     353
|       10 |       5920 |     123
|       11 |       5990 |      70
|       12 |       6018 |      28
|       13 |       6026 |       8
|       14 |       6028 |       2
|       15 |       6028 |       0

To find all the nodes that are reachable at a particular iteration, run the following job:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.FindReachableNodes \
   -input graph-BFS/iter0005 -output graph-BFS/reachable-iter0005

$ hadoop fs -cat 'graph-BFS/reachable-iter0005/part*' | wc
```

These values should be the same as those in the second column of the table above.

To find all the nodes that are at a particular distance (e.g., the search frontier), run the following job:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.FindNodeAtDistance \
   -input graph-BFS/iter0005 -output graph-BFS/d0005 -distance 5

$ hadoop fs -cat 'graph-BFS/d0005/part*' | wc
```

The results are shown in the third column of the table above.

Here's a simple bash script for iterating through the reachability jobs:

```
#!/bin/bash

for i in `seq 0 15`; do
  cur=`echo $i | awk '{printf "%04d\n", $0;}'`
  echo "Iteration $i: reading graph-BFS/iter$cur"
  hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.FindReachableNodes -input graph-BFS/iter$cur -output graph-BFS/reachable-iter$cur
done
```

Here's a simple bash script for extracting nodes at each distance:

```
#!/bin/bash

for i in `seq 0 15`; do
  cur=`echo $i | awk '{printf "%04d\n", $0;}'`
  echo "Iteration $i: reading graph-BFS/iter$cur"
  hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.bfs.FindNodeAtDistance -input graph-BFS/iter$cur -output graph-BFS/d$cur -distance $i
done
```

## PageRank in MapReduce

Make sure you've grabbed the sample graph data (see "Getting Started" above). First, convert the plain-text adjacency list representation into Hadoop `Writable` records:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.pagerank.BuildPageRankRecords \
   -input data/p2p-Gnutella08-adj.txt -output graph-PageRankRecords -numNodes 6301
```

Create the directory where the graph is going to go:

```
$ hadoop fs -mkdir graph-PageRank
```

Partition the graph:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.pagerank.PartitionGraph \
   -input graph-PageRankRecords -output graph-PageRank/iter0000 -numPartitions 5 -numNodes 6301
```

Run 15 iterations:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.pagerank.RunPageRankBasic \
   -base graph-PageRank -numNodes 6301 -start 0 -end 15 -useCombiner
```

Extract the top 20 nodes by PageRank value and examine the results:

```
$ hadoop jar target/bespin-1.1.0-SNAPSHOT-fatjar.jar io.bespin.java.mapreduce.pagerank.FindMaxPageRankNodes \
   -input graph-PageRank/iter0015 -output graph-PageRank-top20 -top 20

$ hadoop fs -cat graph-PageRank-top20/part-r-00000
367     -6.03734
249     -6.12637
145     -6.18742
264     -6.21511
266     -6.23297
123     -6.28525
127     -6.28685
122     -6.29073
1317    -6.29597
5       -6.30274
251     -6.32983
427     -6.33821
149     -6.40216
176     -6.42350
353     -6.43988
390     -6.44404
559     -6.45491
124     -6.45705
4       -6.47055
7       -6.50145
```

Compare the results with a sequential PageRank implementation:

```
$ mvn exec:java -Dexec.mainClass=io.bespin.java.mapreduce.pagerank.SequentialPageRank \
   -Dexec.args="-input data/p2p-Gnutella08-adj.txt -jump 0.15"
```

The results should be the same.
