package ann4s.example

import java.util.UUID

import ann4s._

object AnnoyExample {

  def main(args: Array[String]) {
//    val ints = Array(1, 2, 3, 4)
//    val bytes = Bytes.ints2bytes(ints)
//    val converted = Bytes.bytes2ints(bytes, 0)
//
//    println(converted.toSeq)

    val a = new AnnoyIndex(3, "db")
    a.addItem("0", Array[Float](1, 0, 0), "metadata")
    a.addItem("1", Array[Float](0, 1, 0), "metadata")
    a.addItem("2", Array[Float](0, 0, 1), "metadata")
    a.build(1)

    println(a.getNnsByVector(Array[Float](1.0f, 0.5f, 0.5f), 100).toSeq)

    /*
    val f = 40
    val metric: Metric = Angular // or Euclidean
    val t = new AnnoyIndex(f, metric, "db")  // Length of item vector that will be indexed
    (0 until 1000) foreach { i =>
      val v = Array.fill(f)(scala.util.Random.nextGaussian().toFloat)
      t.addItem(UUID.randomUUID().toString, v, s"[metadata of $i]")
    }
    t.build(10)

    // t.getNnsByItem(0, 1000) runs using HeapByteBuffer (memory)

//    t.save("test.ann") // test.ann is compatible with the native Annoy

    // after `save` t.getNnsByItem(0, 1000) runs using MappedFile (file-based)

    val v = Array.fill(f)(scala.util.Random.nextGaussian().toFloat)
    println(t.getNnsByVector(v, 10).mkString(",")) // will find the 1000 nearest neighbors
    */
  }

}