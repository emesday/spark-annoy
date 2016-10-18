package ann4s

import java.io.FileWriter

import ann4s.profiling.AnnoyDataset
import org.scalatest.{FlatSpec, Matchers}

class AnnoySpec extends FlatSpec with Matchers {

  import AnnoyDataset._

  val f = dataset.head.length
  val i = new AnnoyIndex(f, FixRandom, "db")
  dataset.zipWithIndex.foreach { case (v, j) =>
    i.addItem(j.toString, v, "metadata")
  }

  elapsed("building") {
    i.build(10)
  }

  elapsed(s"qurery") {
//    (0 until 100).foreach { _ =>
      dataset.zipWithIndex.foreach { case (_, j) =>
        val o = i.getNnsByVector(dataset(j), 3)
        print(o.toSeq.map(_._1) + " ")
        println(trueNns(j).toSeq)
//        o.map(_._1) shouldBe trueNns(j)
//      }
    }
  }

  i.close()

}

