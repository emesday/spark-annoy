[![Build Status](https://travis-ci.org/mskimm/annoy4s.svg?branch=master)](https://travis-ci.org/mskimm/annoy4s)

# annoy4s
A Scala Implementation of [Annoy](https://github.com/spotify/annoy).
  
> Annoy (Approximate Nearest Neighbors Oh Yeah) is a C++ library with Python bindings to search for points in space that are close to a given query point. It also creates large read-only file-based data structures that are mmapped into memory so that many processes may share the same data.

# Scala code example

```scala
import annoy4s._

object AnnoyExample {

  def main(args: Array[String]) {
    val f = 40
    val metric: Metric = Angular // or Euclidean
    val t = new AnnoyIndex(f, metric)  // Length of item vector that will be indexed
    (0 until 1000) foreach { i =>
      val v = Array.fill(f)(scala.util.Random.nextGaussian().toFloat)
      t.addItem(i, v)
    }
    t.build(10)

    // t.getNnsByItem(0, 1000) runs using HeapByteBuffer (memory)

    t.save("test.ann") // `test.ann` is compatible with the native Annoy

    // after `save` t.getNnsByItem(0, 1000) runs using MappedFile (file-based)

    println(t.getNnsByItem(0, 1000).mkString(",")) // will find the 1000 nearest neighbors
  }

}

```

# Spark code example

## Item similarity computation
```scala
val dataset: DataFrame = ??? // your dataset
val rank: Int = 50

val alsModel: ALSModel = new ALS()
  .setRank(rank).fit(dataset)

val annoyModel: AnnoyModel = new Annoy()
  .setDimension(rank)
  .fit(alsModel.itemFactors)

val result: DataFrame = annoyModel
  .setK(10) // find 10 neighbors
  .transform(alsModel.itemFactors)

result.show()
```      

The `result.show()` shows

```
+---+--------------------+--------------------+
| id|            features|           neighbors|
+---+--------------------+--------------------+
|  0|[0.00000000, 0.00...|[0, 000, 0000, 00...|
|  1|[0.00000000, 0.00...|[1, 0000, 0000, 0...|
+---+--------------------+--------------------+
```

where `id` and `features` are computed in `ALS`, 
and `neighbors` contains neighbors' ids (replaced to 0s for documentation).

 - For more information of ALS see this [link](http://spark.apache.org/docs/2.0.0/ml-collaborative-filtering.html)


# Installation

```
resolvers += Resolver.bintrayRepo("mskimm", "maven")

libraryDependencies += "com.github.mskimm" %% "annoy4s" % "0.0.3"
```

# References
 - https://github.com/spotify/annoy : native implementation with serveral bindings like Python
 - https://github.com/pishen/annoy4s : Scala wrapper using JNA
 - https://github.com/spotify/annoy-java : Java implementation

