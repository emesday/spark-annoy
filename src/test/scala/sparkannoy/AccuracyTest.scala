package sparkannoy

import java.io.File

import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.sun.jna.Pointer
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.nn.Annoy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import org.scalatest.Matchers.convertNumericToPlusOrMinusWrapper

import scala.collection.JavaConverters._

class AccuracyTest extends FunSuite with DatasetSuiteBase {

  def load(path: String): DataFrame = {
    spark.read
      .parquet(path)
      .select(col("_1").cast("int").as("id"), col("_2").as("features"))
  }

  def getIndex(dataset: String): Pointer = {
    val index = s"dev/test/$dataset.sparkannoy"
    val trainData = load(s"dev/test/parquet/$dataset/train")
    val dim = trainData.first().getAs[Vector](1).size

    if (!new File(index).exists()) {
      val ann = new Annoy().setNumTrees(10)
      val annModel = ann.fit(trainData)
      annModel.saveAsAnnoyBinary(index)
    }

    val annoy = if (dataset.contains("angular")) {
      annoy4s.Annoy.annoyLib.createAngular(dim)
    } else {
      annoy4s.Annoy.annoyLib.createEuclidean(dim)
    }
    annoy4s.Annoy.annoyLib.load(annoy, index)
    annoy
  }

  def testIndex(dataset: String, expectedAccuracy: Double): Unit = {
    import spark.implicits._

    val index = getIndex(dataset)

    val neighbors = load(s"dev/test/parquet/$dataset/neighbors")
      .map { row =>
        val i = row.getInt(0)
        val neighbors = row.getAs[Vector](1).toArray.take(10).map(_.toInt)
        (i, neighbors)
      }
      .collect()
      .sortBy(_._1)
      .map(_._2)

    val data = load(s"dev/test/parquet/$dataset/test")

    var n = 0
    var k = 0
    data.toLocalIterator().asScala.foreach { row =>
      val i = row.getInt(0)
      val vector = row.getAs[Vector](1).toArray.map(_.toFloat)
      val fast = new Array[Int](10)
      val dist = new Array[Float](10)
      annoy4s.Annoy.annoyLib.getNnsByVector(index, vector, 10, 1000, fast, dist)
      n += 10
      k += fast.intersect(neighbors(i)).length
    }
    val accuracy = 100.0 * k / n
    printf("%50s accuracy: %5.2f%% (expected %5.2f%%)\n",
           dataset,
           accuracy,
           expectedAccuracy)

    assert(accuracy === expectedAccuracy +- 1.0)
  }

  test("glove_25") {
    testIndex("glove-25-angular", 69.0)
  }

  test("nytimes_16") {
    testIndex("nytimes-16-angular", 80.0)
  }

  ignore("fashion_mnist: euclidean distance is not implemented") {
    testIndex("fashion-mnist-784-euclidean", 90.0)
  }
}
