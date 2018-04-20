[![Build Status](https://travis-ci.org/mskimm/ann4s.svg?branch=master)](https://travis-ci.org/mskimm/ann4s)
![Maven metadata URI](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/com/github/mskimm/ann4s_2.11/maven-metadata.xml.svg)

# Ann4s

Building [Annoy](https://github.com/spotify/annoy) Index on Apache Spark. Then query neighbors using Annoy.

# Note

I had built an index of 117M 64-dimensional vectors using 100 nodes in 5 minutes. The settings was;
```
// version: 0.1.4
// spark.executor.instances = 100
// spark.executor.memory = 8g
// spark.driver.memory = 8g
val fraction = 0.00086 // for about 100k samples
val numTrees = 2
val numPartitions = 100
val annoyModel = new Annoy().setFraction(fraction).setNumTrees(numTrees).fit(dataset)
annoyModel.saveAsAnnoyBinary("/hdfs/path/to/index", numPartitions)
```

The size of the index is about 33G.

# Distributed Builds

```scala
import spark.implicits._

val data = spark.read.textFile("data/annoy/sample-glove-25-angular.txt")
  .map { str =>
    val Array(id, features) = str.split("\t")
    (id.toInt, features.split(",").map(_.toFloat))
  }
  .toDF("id", "features")

val ann = new Annoy()
  .setNumTrees(2)

val annModel = ann.fit(data)

annModel.saveAsAnnoyBinary("/path/to/dump/annoy-binary")
```

# Dependency

From the version 0.1.2, it is released to Maven.

```
libraryDependencies += "com.github.mskimm" %% "ann4s" % "0.1.4"
```
 - `0.1.4` is built with Apache Spark 2.3.0
 
# How does it work?

1. builds a parent tree using sampled data on Spark Master
2. all data are grouped by the leaf node of parent tree on Spark Nodes
3. builds subtree using the grouped data on each Spark Nodes
4. aggregate all nodes of subtree to parent tree on Spark Master
 
# Use Case

## Index ALS User/Item Factors
 - src/test/scala/ann4s/spark/example/ALSBasedUserItemIndexing.scala
 
```scala
...
val training: DataFrame = _
val als = new ALS()
  .setMaxIter(5)
  .setRegParam(0.01)
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")

val model = als.fit(training)

val ann = new Annoy()
  .setNumTrees(2)
  .setFraction(0.1)
  .setIdCol("id")
  .setFeaturesCol("features")

val userAnnModel= ann.fit(model.userFactors)
userAnnModel.writeAnnoyBinary("exp/als/user_factors.ann")

val itemAnnModel = ann.fit(model.itemFactors)
itemAnnModel.writeAnnoyBinary("exp/als/item_factors.ann")
...
```

# Comment

I personally started this project to study Scala. I found out that Annoy
is a fairly good library for nearest neighbors search and can be implemented
distributed version using Apache Spark. Recently, various bindings and
implementations have been actively developed. In particular, the purpose
and usability of this project overlap with some projects like
[annoy4s](https://github.com/annoy4s/annoy4s) and
[annoy-java](https://github.com/spotify/annoy-java) in terms of running on JVM. 

To continue contribution, from now on this project focuses on building Index 
on Apache Spark for distributed builds. This will support building using 
1 billion or more items and writing Annoy compatible binary.

# References

 - https://github.com/spotify/annoy : native implementation with serveral bindings like Python
 - https://github.com/pishen/annoy4s : Scala wrapper using JNA
 - https://github.com/spotify/annoy-java : Java implementation
