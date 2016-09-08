package annoy4s

import org.scalatest.{FlatSpec, Matchers}

class AnnoySpec extends FlatSpec with Matchers {
  /**
    * Reallocating to 1 nodes
    * Reallocating to 2 nodes
    * Reallocating to 3 nodes
    * pass 0...
    * Reallocating to 5 nodes
    * pass 1...
    * pass 2...
    * Reallocating to 7 nodes
    * pass 3...
    * pass 4...
    * Reallocating to 10 nodes
    * pass 5...
    * pass 6...
    * pass 7...
    * Reallocating to 14 nodes
    * pass 8...
    * pass 9...
    * Reallocating to 23 nodes
    * has 23 nodes
    */
  it should "test_get_nns_by_item" in {
    val f = 3
    val i = new AnnoyIndex(f)
    i.verbose(true)
    i.addItem(0, Array[Float](2, 1, 0))
    i.addItem(1, Array[Float](1, 2, 0))
    i.addItem(2, Array[Float](0, 0, 1))
    i.build(10)

    i.getNnsByItem(0, 3).map(_._1) shouldBe Array(0, 1, 2)
    i.getNnsByItem(1, 3).map(_._1) shouldBe Array(1, 0, 2)
    Array(i.getNnsByItem(2, 3).map(_._1)) should contain oneOf(Array(2, 0, 1), Array(2, 1, 0))
  }

//  it should "build/query time" in {
//    val dimension = 50
//    val numItems = 10000
//    val numTrials = 1
//
//    var s = System.currentTimeMillis()
//    val vectors = (0 until numItems).map { i =>
//      (i, Array.fill(dimension)(scala.util.Random.nextFloat()))
//    }.toSeq
//
//    s = System.currentTimeMillis()
//    val scalaVersion = new AnnoyIndex(dimension)
//    vectors.foreach { case (i, v) =>
//      scalaVersion.addItem(i, v)
//    }
//    scalaVersion.build(-1)
//    println(s"scala build ${System.currentTimeMillis() - s} ms")
//
//    s = System.currentTimeMillis()
//    (0 until numTrials).foreach { trial =>
//      (0 until numItems).foreach { item =>
//        scalaVersion.getNnsByItem(item, 100, -1)
//      }
//    }
//    println(s"scala query ${System.currentTimeMillis() - s} ms")
//  }
}
