package ann4s

import java.io.FileWriter

import ann4s.profiling.AnnoyDataset
import org.scalatest.{FlatSpec, Matchers}

class AnnoySpec extends FlatSpec with Matchers {

  import AnnoyDataset._

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

  it should "building index for Py" in {
    val f = dataset.head.length
    val angular = new AnnoyIndex(f, Angular)
    val euclidean = new AnnoyIndex(f, Euclidean)
    dataset.zipWithIndex.foreach { case (v, j) =>
      angular.addItem(j, v)
      euclidean.addItem(j, v)
    }
    angular.build(10)
    angular.save("src/test/resources/annoy-index-angular-scala")
    val s = (0 until angular.getNItems).map { item =>
      angular.getNnsByItem(item, 10).map(_._1).mkString("[", ", ", "]")
    }
    val angularWriter = new FileWriter("src/test/py/scala_angular_result.py")
    angularWriter.write("result = " + s.mkString("[", ",\n          ", "]"))
    angularWriter.close()
    angular.unload()

    euclidean.build(10)
    euclidean.save("src/test/resources/annoy-index-euclidean-scala")
    (0 until angular.getNItems).map { item =>
      euclidean.getNnsByItem(item, 10).map(_._1).mkString("[", ", ", "]")
    }
    val euclideanWiter = new FileWriter("src/test/py/scala_euclidean_result.py")
    euclideanWiter.write("result = " + s.mkString("[", ",\n          ", "]"))
    euclideanWiter.close()
    euclidean.unload()
  }

  it should "Py Angular" in {
    val pyIndex = new AnnoyIndex(10, Angular)
    pyIndex.verbose(true)
    pyIndex.load(getClass.getResource("/annoy-index-angular-py").getPath)
    (0 until pyIndex.getNItems).foreach { item =>
      pyIndex.getNnsByItem(item, 10).map(_._1) shouldBe PyAngularResult.result(item)
    }
  }

  it should "Py Euclidean" in {
    val pyIndex = new AnnoyIndex(10, Euclidean)
    pyIndex.verbose(true)
    pyIndex.load(getClass.getResource("/annoy-index-euclidean-py").getPath)
    (0 until pyIndex.getNItems).foreach { item =>
      pyIndex.getNnsByItem(item, 10).map(_._1) shouldBe PyEuclideanResult.result(item)
    }
  }
}

