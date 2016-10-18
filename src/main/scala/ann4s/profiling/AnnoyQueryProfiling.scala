//package ann4s.profiling
//
//import ann4s.AnnoyIndex
//
//object AnnoyQueryProfiling {
//
//  import AnnoyDataset._
//
//  def main(args: Array[String]) {
//
//    val f = dataset.head.length
//    val i = new AnnoyIndex(f)
//    i.load("annoy-index-scala", useHeap = false)
//    elapsed("query ... ", 5000, logging = true) {
//      dataset.zipWithIndex.foreach { case (_, j) =>
//        i.getNnsByItem(j, 3)
//      }
//    }
//    i.unload()
//
//  }
//}
//
