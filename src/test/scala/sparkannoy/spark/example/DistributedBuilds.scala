package ann4s.spark.example

import ann4s.spark.LocalSparkApp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.nn.{Annoy, AnnoyModel}
import org.apache.spark.sql.SparkSession

object DistributedBuilds extends LocalSparkApp {

  override def run(spark: SparkSession): Unit = {
    import spark.implicits._

    Logger.getLogger("org.apache.spark.ml.nn").setLevel(Level.DEBUG)

    val data = spark.read.textFile("data/annoy/sample-glove-25-angular.txt")
      .map { str =>
        val Array(id, features) = str.split("\t")
        (id.toInt, features.split(",").map(_.toFloat))
      }
      .toDF("id", "features")

    val ann = new Annoy()
      .setNumTrees(2)

    val annModel = ann.fit(data)

    annModel.write.overwrite().save("exp/ann")

    val loaded = AnnoyModel.load("exp/ann")

    // saving all together
    // this is compatible with Annoyt
    loaded.saveAsAnnoyBinary("exp/annoy/spark.ann", overwrite = true)

    // saving partitioned data
    // should be merged using `cat` then it is compatible with Annoy
    loaded.saveAsAnnoyBinary("exp/annoy/spark.partitioned", spark.sparkContext.defaultParallelism, overwrite = true)

    println("$ cat exp/annoy/spark.partitioned.* > exp/annoy/spark.merged.ann")
    println("or")
    println("$ hadoop fs -cat exp/annoy/spark.partitioned.* > spark.merged.ann")
  }

}

