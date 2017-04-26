[![Build Status](https://travis-ci.org/mskimm/ann4s.svg?branch=master)](https://travis-ci.org/mskimm/ann4s)
[![Analytics](https://ga-beacon.appspot.com/UA-98128111-1/ann4s)](https://github.com/igrigorik/ga-beacon)

# Ann4s
A Scala Implementation of [Annoy](https://github.com/spotify/annoy) which searches nearest neighbors given query point. 

Ann4s also provides [DataFrame-based API](http://spark.apache.org/docs/latest/ml-guide.html) 
for [Apache Spark](https://spark.apache.org/).

# Scala code example

```scala
import ann4s._

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

# Spark code example (with DataFrame-based API)

## Item similarity computation
```scala
val dataset: DataFrame = ??? // your dataset

val alsModel: ALSModel = new ALS()
  .fit(dataset)

val annoyModel: AnnoyModel = new Annoy()
  .setDimension(alsModel.rank)
  .fit(alsModel.itemFactors)

val result: DataFrame = annoyModel
  .setK(10) // find 10 neighbors
  .transform(alsModel.itemFactors)

result.show()
```      

The `result.show()` shows

```
+---+--------+-----------+
| id|neighbor|   distance|
+---+--------+-----------+
|  0|       0|        0.0|
|  0|      50|0.014339785|
...
|  1|       1|        0.0|
|  1|      36|0.011467933|
...
+---+--------+-----------+
```

 - For more information of ALS see this [link](http://spark.apache.org/docs/2.0.0/ml-collaborative-filtering.html)
 - Working example is at 'src/test/scala/ann4s/spark/AnnoySparkSpec.scala'

# Installation

```
resolvers += Resolver.bintrayRepo("mskimm", "maven")

libraryDependencies += "com.github.mskimm" %% "ann4s" % "0.0.6"
```
 - `0.0.6` is built with Apache Spark 1.6.2

# References
 - https://github.com/spotify/annoy : native implementation with serveral bindings like Python
 - https://github.com/pishen/annoy4s : Scala wrapper using JNA
 - https://github.com/spotify/annoy-java : Java implementation

