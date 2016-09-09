package annoy4s

import annoy4s.profiling.AnnoyDataset
import org.scalatest.{FlatSpec, Matchers}

class AnnoySpec extends FlatSpec with Matchers {

  import AnnoyDataset._

  object FixRandom extends Random {
    val rnd = new scala.util.Random(0)
    override def flip(): Boolean = rnd.nextBoolean()
    override def index(n: Int): Int = rnd.nextInt(n)
  }

  it should "" in {
    val f = dataset.head.length
    val i = new AnnoyIndex(f, FixRandom)
    dataset.zipWithIndex.foreach { case (v, j) =>
      i.addItem(j, v)
    }

    elapsed("building") {
      i.build(10)
    }

    elapsed(s"qurery on ${i.getBufferType}") {
      (0 until 100).foreach { _ =>
        dataset.zipWithIndex.foreach { case (_, j) =>
          val o = i.getNnsByItem(j, 3)
          o.map(_._1) shouldBe trueNns(j)
        }
      }
    }

    elapsed("saving / unload / load") {
      i.save("annoy-index-scala")
    }

    elapsed(s"qurery on ${i.getBufferType}") {
      (0 until 100).foreach { _ =>
        dataset.zipWithIndex.foreach { case (_, j) =>
          val o = i.getNnsByItem(j, 3)
          o.map(_._1) shouldBe trueNns(j)
        }
      }
    }

    elapsed("load (useHeap = false)") {
      i.unload()
      i.load("annoy-index-scala")
    }

    elapsed(s"qurery on ${i.getBufferType}") {
      (0 until 100).foreach { _ =>
        dataset.zipWithIndex.foreach { case (_, j) =>
          val o = i.getNnsByItem(j, 3)
          o.map(_._1) shouldBe trueNns(j)
        }
      }
    }

    elapsed("load (useHeap = true)") {
      i.unload()
      i.load("annoy-index-scala", true)
    }

    elapsed(s"qurery on ${i.getBufferType}") {
      (0 until 100).foreach { _ =>
        dataset.zipWithIndex.foreach { case (_, j) =>
          val o = i.getNnsByItem(j, 3)
          o.map(_._1) shouldBe trueNns(j)
        }
      }
    }
  }
}

