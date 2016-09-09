# annoy4s
A Scala Implementation of [Annoy](https://github.com/spotify/annoy).

This can solve nearest neighbors (ann or knn) problems on [Apache Spark](https://spark.apache.org/) and [Apache S2Graph (incubating)](http://s2graph.incubator.apache.org/).
  
> Annoy (Approximate Nearest Neighbors Oh Yeah) is a C++ library with Python bindings to search for points in space that are close to a given query point. It also creates large read-only file-based data structures that are mmapped into memory so that many processes may share the same data.

# Scala code example

```scala
import annoy4s.AnnoyIndex

object AnnoyTest {

  def main(args: Array[String]) {
    val f = 40
    val t = new AnnoyIndex(f)  // Length of item vector that will be indexed
    (0 until 1000) foreach { i =>
      val v = Array.fill(f)(scala.util.Random.nextGaussian().toFloat)
      t.addItem(i, v)
    }
    t.build(10)
    
    // t.getNnsByItem(0, 1000) runs using HeapByteBuffer (memory)
    
    t.save("test.ann") // test.ann is compatible with the native Annoy
    
    // after `save` t.getNnsByItem(0, 1000) runs using MappedFile (file-based)
    
    println(t.getNnsByItem(0, 1000).mkString(",")) // will find the 1000 nearest neighbors
  }

}

```

# Spark code example

## Item similarity computation
```scala
object AnnoyLoader {
  // singleton on every executors
  var annoy: AnnoyIndex = _
  def getAnnoy(dim: Int, filename: String): AnnoyIndex = {
    if (annoy == null) {
      annoy = new AnnoyIndex(dim)
      annoy.load(filename)
    }
    annoy
  }
}

val dataset: DataFrame = ???
val rank: Int = 50
// This will be the size of `item.toLocalIterator`
// which is the number of partitions.
val numItemBlocks: Int = 100
val annoyIndexFilename: String = "annoy-index"

val alsModel: ALSModel = 
  new ALS().setRank(rank).setNumItemBlocks(numItemBlocks).fit(dataset)

val itemFactors: RDD[(Int, Array[Float])] = 
  alsModel.itemFactors.map { case Row(id: Int, features: Seq[_]) =>
    (id, features.asInstanceOf[Seq[Float]].toArray)
  }

// Build a Annoy index on the Driver.
val annoyOnDriver: AnnoyIndex = new AnnoyIndex(rank)

// The iterator will consume as much memory as the largest partition in this RDD.
itemFactors.toLocalIterator.foreach { case (id, v) =>
  annoyOnDriver.addItem(id, v)
}

// build and save
annoyOnDriver.build(10)
annoyOnDriver.save(annoyIndexFilename)

// Add the index file to be downloaded on every executors.
dataset.sqlContext.sparkContext.addFile(annoyIndexFilename)

// nn computing on Executors
val itemSimilarity: RDD[(Int, Array[(Int, Float)])] = 
  itemFactors.keys.map(x => (x, AnnoyLoader.getAnnoy(rank, annoyIndexFilename).getNnsByItem(x, 10)))
```      
 - For more information of ALS see this [link](http://spark.apache.org/docs/2.0.0/ml-collaborative-filtering.html)


# Installation

```
resolvers += Resolver.bintrayRepo("mskimm", "maven")

libraryDependencies += "com.github.mskimm" %% "annoy4s" % "0.0.1"
```

# TODO
  - Angular: Done
  - save: Done
  - load/unload: Done
  - optimization: Done
  - Spark code example: Done
  - Euclidean: TBD

# References
 - https://github.com/spotify/annoy : native implementation with serveral bindings like Python
 - https://github.com/pishen/annoy4s : Scala wrapper using JNA
 - https://github.com/spotify/annoy-java : Java implementation

