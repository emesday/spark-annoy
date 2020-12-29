package sparkannoy.spark
import org.apache.spark.sql.SparkSession

object CollectTest extends LocalSparkApp {
  override def run(spark: SparkSession): Unit = {
    val rdd = spark.sparkContext.parallelize(Seq(0, 1, 2, 3))
    val worker = rdd.mapPartitionsWithIndex { case (i, it) =>
      val s = System.currentTimeMillis()
      Thread.sleep(5000)
      val e = System.currentTimeMillis()
      Iterator.single((i, it.size, s, e))
    }
    worker.collect().foreach(println) // simultaneously
    /*
    (0,1,1524119825194,1524119830196)
    (1,1,1524119825194,1524119830196)
    (2,1,1524119825194,1524119830196)
    (3,1,1524119825194,1524119830196)
    */
    worker.toLocalIterator.foreach(println) // sequentially.
    /*
    (0,1,1524119830315,1524119835315)
    (1,1,1524119835340,1524119840345)
    (2,1,1524119840366,1524119845369)
    (3,1,1524119845391,1524119850392)
     */
  }
}
