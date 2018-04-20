package ann4s

import java.io._

import scala.util.Random

object LocalBuilds {

  val d = 25

  def readDataset(): IndexedSeq[IdVectorWithNorm] = {
    val is = try { new FileInputStream(s"data/train.bin") }
    catch { case _: Throwable => throw new Error("run `data/download.sh` in shell first") }

    val dis = new DataInputStream(new BufferedInputStream(is))
    val n = dis.readInt()
    val d = dis.readInt()
    val data = Array.fill(n)(Array.fill(d)(dis.readFloat)).zipWithIndex.map {
      case (v, i) => IdVectorWithNorm(i, v)
    }
    is.close()
    data
  }

  def main(args: Array[String]): Unit = {

    val items = readDataset()
    val d = items.head.vector.size

    val fraction = 0.01
    val numSamples = (fraction * items.length).toInt

    val globalAggregator = new IndexAggregator

    val samples = Array.fill(numSamples)(items(Random.nextInt(items.length)))

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
        .par
        .map { case (subTreeId, it) =>
          subTreeId -> new IndexBuilder(1, d + 2).build(it.map(_._2))
        }
        .seq
        .foreach { case (subTreeId, subTreeNodes) =>
          localAggregator.mergeSubTree(subTreeId, subTreeNodes.nodes)
        }

      globalAggregator.aggregate(localAggregator.nodes)
    }

    val index = globalAggregator.result()

    val directory = new File("exp/annoy")
    directory.mkdirs()
    val os = new FileOutputStream(new File(directory, "local.ann"))
    AnnoyUtil.dump(d, items.toIterator, index.getNodes, os)
    os.close()
  }

}
