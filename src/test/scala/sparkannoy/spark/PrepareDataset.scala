package ann4s.spark

import java.io.{BufferedInputStream, DataInputStream, FileInputStream}

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object PrepareDataset extends LocalSparkApp {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val is = try { new FileInputStream(s"data/train.bin") }
    catch { case _: Throwable => throw new Error("run `data/download.sh` in shell first") }

    val dis = new DataInputStream(new BufferedInputStream(is))
    val n = dis.readInt()
    val d = dis.readInt()

    val data = Array.fill(n)(Vectors.dense(Array.fill(d)(dis.readFloat.toDouble))).zipWithIndex

    spark
      .sparkContext
      .parallelize(data)
      .toDF("features", "id")
      .write
      .mode("overwrite")
      .parquet(s"data/train")

  }

}
