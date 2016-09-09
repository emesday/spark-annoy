package annoy4s

import org.scalatest.{FlatSpec, Matchers}

class AnnoyBuildProfiling extends FlatSpec with Matchers {

  import AnnoyDataset._

  Thread.sleep(3000L) // to run VisualVM

  it should "profiling `build`" in {
    val f = dataset.head.length
    elapsed("build", 3000) {
      val i = new AnnoyIndex(f)
      dataset.zipWithIndex.foreach { case (v, j) =>
        i.addItem(j, v)
      }
      i.build(10)
      i.save("annoy-index-scala")
      i.unload()
    }
  }
}
