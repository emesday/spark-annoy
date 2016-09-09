package annoy4s.example

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

    t.save("test.ann") // test.ann is compatible with the native Annoy

    // after `save` t.getNnsByItem(0, 1000) runs using MappedFile (file-based)

    println(t.getNnsByItem(0, 1000).mkString(",")) // will find the 1000 nearest neighbors
  }

}