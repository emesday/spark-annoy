package sparkannoy.spark.example

import sparkannoy.spark.LocalSparkApp
import org.apache.spark.ml.nn.Annoy
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

object ALSBasedUserItemIndexing extends LocalSparkApp {

  override def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val ratings = spark.read.textFile("data/mllib/als/sample_movielens_ratings.txt")
      .map { str =>
        val fields = str.split("::")
        assert(fields.size == 4)
        Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
      }
      .toDF()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)

    val ann = new Annoy()
      .setNumTrees(2)
      .setFraction(0.1)
      .setIdCol("id")
      .setFeaturesCol("features")

    val userAnnModel= ann.fit(model.userFactors)
    userAnnModel.saveAsAnnoyBinary("exp/als/user_factors.ann")

    val itemAnnModel = ann.fit(model.itemFactors)
    itemAnnModel.saveAsAnnoyBinary("exp/als/item_factors.ann")
  }
}
