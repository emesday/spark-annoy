package ann4s.spark

import java.io.{BufferedInputStream, DataInputStream, FileInputStream}

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object PrepareDataset {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("Prepare Dataset")
      .getOrCreate()

    val is = try { new FileInputStream(s"dataset/train.bin") }
    catch { case _: Throwable => throw new Error("run `dataset/download.sh` in shell first") }

    val dis = new DataInputStream(new BufferedInputStream(is))
    val n = dis.readInt()
    val d = dis.readInt()

    val dataset = Array.fill(n)(Vectors.dense(Array.fill(d)(dis.readFloat.toDouble))).zipWithIndex

    import spark.implicits._

    spark
      .sparkContext
      .parallelize(dataset)
      .toDF("features", "id")
      .write
      .mode("overwrite")
      .parquet(s"dataset/train")

    spark.stop()
  }

}
