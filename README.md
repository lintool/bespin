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

**Tip:** `sed` does not accept control characters such as `\t`, so you have to insert a literal tab in the command lin. To do so on Mac OS X, type `^V^I`.

