package ann4s.spark.example

import ann4s.spark.LocalSparkApp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.nn.Annoy

case class MovieLensData(user: Int, item: Int, rating: Int, timestamp: Long)

object MovieLens extends LocalSparkApp {
  override def run(spark: SparkSession): Unit = {
    Logger.getLogger("ann4s").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.ml.nn").setLevel(Level.DEBUG)

    import spark.implicits._
    val schema = Encoders.product[MovieLensData].schema

    val dataset = spark.read.schema(schema)
      .option("delimiter", "\t")
      .csv("data/ml-100k/u.data.gz")

    val als = new ALS().setRank(10)
    val model = als.fit(dataset)

    val ann = new Annoy()
      .setNumTrees(2)
      .setFraction(0.1)
      .setIdCol("id")
      .setFeaturesCol("features")

    model.userFactors.rdd
      .map { case Row(id: Int, features: Seq[_]) => (id, features.mkString(" ")) }
      .toDF("id", "features").repartition(1).write.mode("overwrite").csv("exp/ml-100k/als/user_factors")
    val userAnnModel= ann.fit(model.userFactors)
    userAnnModel.saveAsAnnoyBinary("exp/ml-100k/als/user_factors.ann")

    model.userFactors.rdd
      .map { case Row(id: Int, features: Seq[_]) => (id, features.mkString(" ")) }
      .toDF("id", "features").repartition(1).write.mode("overwrite").csv("exp/ml-100k/als/item_factors")
    val itemAnnModel = ann.fit(model.itemFactors)
    itemAnnModel.saveAsAnnoyBinary("exp/ml-100k/als/item_factors.ann")
  }
}
