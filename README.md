# annoy4s
A Scala Implementation of [Annoy](https://github.com/spotify/annoy)

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

# Install

Just add AnnoyIndex.scala file to your project. (at this time ...)

# Annoy for Spark

coming soon...

# TODO
  - Angular: Done
  - save: Done
  - load/unload: Done
  - optimization: WIP
  - Spark Examples: TBD
  - Euclidean: TBD
  - All features in Annoy: TBD

# References
 - https://github.com/spotify/annoy : native implementation with serveral bindings like Python
 - https://github.com/pishen/annoy4s : Scala wrapper using JNA
 - https://github.com/spotify/annoy-java : Java implementation

