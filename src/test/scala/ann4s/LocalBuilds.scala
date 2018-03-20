package ann4s

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object LocalBuilds {

  val d = 25

  def readDataset(): IndexedSeq[IdVectorWithNorm] = {
    val data = new Array[Byte](d * 4)
    val ar = new Array[Float](d)

    val fis = new FileInputStream(s"dataset/train.bin")
    val items = new ArrayBuffer[IdVectorWithNorm]()
    var id = 0
    while (fis.read(data) == d * 4) {
      val bf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN)
      var i = 0
      while (i < d) {
        ar(i) = bf.getFloat()
        i += 1
      }

      val copied = ar.map(_.toDouble)
      val cv = Vector64(copied)
      items += IdVectorWithNorm(id, cv)
      id += 1
      if ((id % 10000) == 0)
        println(id)
    }
    fis.close()
    items
  }

  def main(args: Array[String]): Unit = {

    val items = readDataset()

    val globalAggregator = new IndexAggregator

    val samples = Array.fill(10000)(items(Random.nextInt(items.length)))

    implicit val random: Random = new Random
    implicit val distance: Distance = CosineDistance

    0 until 2 foreach { _ =>

      val builder = new IndexBuilder(1, d + 2)
      val parentTree = builder.build(samples)
      val localAggregator = new IndexAggregator().aggregate(parentTree.nodes)

      items
        .map { item =>
          parentTree.traverse(item.vector) -> item
        }
        .groupBy(_._1)
        .map { case (subTreeId, it) =>
          println(subTreeId, it.length)
          subTreeId -> new IndexBuilder(1, d + 2).build(it.map(_._2))
        }
        .foreach { case (subTreeId, subTreeNodes) =>
          localAggregator.mergeSubTree(subTreeId, subTreeNodes.nodes)
        }

      globalAggregator.aggregate(localAggregator.nodes)
    }

    val index = globalAggregator.result()

    val directory = new File("exp/annoy")
    directory.mkdirs()
    val os = new FileOutputStream(new File(directory, "local.ann"))
    AnnoyUtil.dump(items, index.getNodes, os)
    os.close()
  }

}
