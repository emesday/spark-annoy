package annoy4s.profiling

import annoy4s.AnnoyIndex

object AnnoyQueryProfiling {

  import AnnoyDataset._

  def main(args: Array[String]) {

    Thread.sleep(3000L) // to run VisualVM

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

