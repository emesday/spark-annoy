package ann4s.spark.example

import ann4s.spark.LocalSparkApp
import org.apache.spark.ml.nn.Annoy
import org.apache.spark.sql.SparkSession

object FastTextIndexing extends LocalSparkApp {
  override def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val data = spark.sparkContext.textFile("data/fasttext/cc.ko.300.vec.sample.gz")
      .mapPartitionsWithIndex {
        case (i, it) if i == 0 => it.drop(1) // drop header
        case (_, it) => it
      }
      .zipWithIndex()
      .map { case (str, id) =>
        val Array(word, xs@_*) = str.split(" ")
        val features = xs.map(_.toFloat).toArray
        (id.toInt, word, features)
      }
      .toDF("id", "word", "features")

    val ann = new Annoy()
      .setNumTrees(2)
      .setFraction(0.1)
      .setIdCol("id")
      .setFeaturesCol("features")

    val userAnnModel= ann.fit(data)
    userAnnModel.writeAnnoyBinary("exp/fasttext/cc.ko.300.vec.ann")
  }
}
