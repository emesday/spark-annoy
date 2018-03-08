package ann4s

import java.io.FileInputStream
import java.nio.{ByteBuffer, ByteOrder}

object AnnoyIndexTest {

  val d = 25

  def main(args: Array[String]): Unit = {


    val data = new Array[Byte](d * 4)
    val ar = new Array[Float](d)

    // builds
    if (false) {
      val index = new AnnoyIndex(d, CosineDistance)
      val fis = new FileInputStream(s"dataset/train.bin")
      var id = 0
      while (fis.read(data) == d * 4) {
        val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < d) {
          ar(i) = bf.getFloat()
          i += 1
        }
        index.addItem(id, ar)
        id += 1
      }
      fis.close()

      index.build(1)
      index.save("exp/annoy.ann")
      println("done")

    }

    // query
    if (true) {
      val index = new AnnoyIndex(d, CosineDistance)
      index.load("exp/annoy.ann")
      var n = 1000
      val fis = new FileInputStream(s"dataset/test.bin")
      while (n >= 0 && fis.read(data) == d * 4) {
        val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
        var i = 0
        while (i < d) {
          ar(i) = bf.getFloat()
          i += 1
        }
        val nns = index.getNnsByVector(ar, 100, 10000)
        println(nns.map(_._1).toSeq)
        n -= 1
      }
      fis.close()
    }

  }

}
