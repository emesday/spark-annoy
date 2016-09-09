package annoy4s

import org.scalatest.{FlatSpec, Matchers}

class AnnoyQueryProfiling extends FlatSpec with Matchers {

  import AnnoyDataset._

  Thread.sleep(3000L) // to run VisualVM

  it should "profiling `query`" in {
    val f = dataset.head.length
    val i = new AnnoyIndex(f)
    i.load("annoy-index-scala", useHeap = false)
    elapsed("query ... ", 5000) {
      dataset.zipWithIndex.foreach { case (_, j) =>
        i.getNnsByItem(j, 3)
      }
    }
    i.unload()
  }
}

