package ann4s

import java.io.FileInputStream
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer

object RocksDBApp {

  val d = 25

  def readDataset(): IndexedSeq[(Int, Vector)] = {
    val data = new Array[Byte](d * 4)
    val ar = new Array[Float](d)

    val fis = new FileInputStream(s"dataset/test.bin")
    val items = new ArrayBuffer[(Int, Vector)]()
    var id = 0
    while (fis.read(data) == d * 4) {
      val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      while (i < d) {
        ar(i) = bf.getFloat()
        i += 1
      }

      val cv = Vector32(ar.clone())
      items += id -> cv
      id += 1
    }
    fis.close()
    items
  }

  def main(args: Array[String]): Unit = {

    val data = readDataset()
    val index = Index.fromRocksDB("exp/rocksdb")
    implicit val distance: Distance = CosineDistance

    data.take(1) foreach { case (i, v) =>
      println(index.getNns(v, 100, 10000).toSeq)
    }

  }

}
