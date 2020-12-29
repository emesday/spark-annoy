package ann4s.spark

import org.apache.spark.sql.SparkSession

abstract class LocalSparkApp {

  def run(spark: SparkSession): Unit

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("Prepare Dataset")
      .getOrCreate()

    run(spark)

    spark.stop()
  }

}
