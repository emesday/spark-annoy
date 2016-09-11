package ann4s.spark

import ann4s.Random
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{FlatSpec, Matchers}

class AnnoySparkSpec extends FlatSpec with Matchers with LocalSparkContext {

  import ann4s.profiling.AnnoyDataset.{dataset => features, trueNns}

  object FixRandom extends Random {
    val rnd = new scala.util.Random(0)
    override def flip(): Boolean = rnd.nextBoolean()
    override def index(n: Int): Int = rnd.nextInt(n)
  }

  "Spark ML API" should "work" in {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val idCol = "id"
    val featuresCol = "features"
    val neighborCol = "neighbor"
    val dimension = features.head.length

    val rdd: RDD[(Int, Array[Float])] =
      sc.parallelize(features.zipWithIndex.map(_.swap))

    val dataset: DataFrame = rdd.toDF(idCol, featuresCol)

    val annoyModel: AnnoyModel = new Annoy()
      .setDimension(dimension)
      .setIdCol(idCol)
      .setFeaturesCol(featuresCol)
      .setNeighborCol(neighborCol)
      .setDebug(true)
      .fit(dataset)

    val result: DataFrame = annoyModel
      .setK(10) // find 10 neighbors
      .transform(dataset)

    result.show()

    result.select(idCol, neighborCol)
      .map { case Row(id: Int, neighbor: Int) =>
        (id, neighbor)
      }
      .groupByKey()
      .collect()
      .foreach { case (id, nns) =>
        nns.toSeq.intersect(trueNns(id)).length should be >= 2
      }
  }

}

