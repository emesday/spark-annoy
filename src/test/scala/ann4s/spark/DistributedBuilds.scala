package ann4s.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.nn.{Annoy, AnnoyModel}
import org.apache.spark.sql.SparkSession

object DistributedBuilds {

  def main(args: Array[String]): Unit = {

    // turn off log
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org.apache.spark.ui").setLevel(Level.DEBUG)
    Logger.getLogger("org.apache.spark.ml").setLevel(Level.DEBUG)

    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("distributed builds")
      .getOrCreate()

    val data = spark.read.parquet("dataset/train")

    val ann = new Annoy()
      .setNumTrees(2)

    val annModel = ann.fit(data)

    annModel.write.overwrite().save("exp/ann")

    val loaded = AnnoyModel.load("exp/ann")

    loaded.writeAnnoyBinary("exp/annoy/spark.ann")

    spark.stop()
  }

}

