package ann4s.spark

import java.io.FileInputStream
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.ml.linalg.{Vectors, Vector}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object PrepareDataset {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[4]")
      .appName("Prepare Dataset")
      .getOrCreate()

    import spark.implicits._

    val d = 25
    val data = new Array[Byte](d * 4)
    val ar = new Array[Float](d)

    Seq("train", "test") foreach { setName =>
      val fis = try {
        new FileInputStream(s"dataset/$setName.bin")
      } catch { case ex: Throwable =>
        println("run `dataset/download.sh` in shell first")
        throw ex
      }

      val dataset = new ArrayBuffer[(Int, Vector)]()
      var id = 0
      while (fis.read(data) == d * 4) {
        val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < d) {
          ar(i) = bf.getFloat()
          i += 1
        }

        val cv = Vectors.dense(ar.map(_.toDouble))
        dataset += id -> cv
        id += 1
        if ((id % 10000) == 0)
          println(id)
      }
      fis.close()

      spark
        .sparkContext
        .parallelize(dataset)
        .toDF("id", "features")
        .write
        .mode("overwrite")
        .parquet(s"dataset/$setName")
    }

    spark.stop()
  }

}
