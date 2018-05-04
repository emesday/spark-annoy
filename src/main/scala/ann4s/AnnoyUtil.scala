package ann4s

import java.io.OutputStream
import java.nio.{ByteBuffer, ByteOrder}

case class PartInfo(partId: Int, firstId: Int, lastId: Int)

case class AnnoyUtilItemStat(part: Seq[PartInfo], numItems: Int, numHoles: Int, numBytesWritten: Long) {
  def +(o: AnnoyUtilItemStat): AnnoyUtilItemStat =
    copy(part ++ o.part, numItems + o.numItems, numHoles + o.numHoles, numBytesWritten + o.numBytesWritten)
}

case class AnnoyUtilNodeStat(
  offset: Int, numRootNodes: Int, numHyperplaneNodes: Int, numLeafNodes: Int, numBytesWritten: Long)

object AnnoyUtil {

  def saveNodes(nodes: Nodes, d: Int, offset: Int, os: OutputStream): AnnoyUtilNodeStat = {
    var numBytesWritten = 0L
    val write = { b: Array[Byte] =>
      os.write(b)
      numBytesWritten += b.length
    }
    val buffer = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)
    val numItemNodes = offset
    var numRootNodes = 0
    var numHyperplaneNodes = 0
    var numLeafNodes = 0
    nodes.nodes foreach {
      case RootNode(location) =>
        assert(numItemNodes > 0 && numHyperplaneNodes > 0 && numLeafNodes > 0)
        nodes.nodes(math.abs(location)) match {
          case InternalNode(l, r, hyperplane) =>
            buffer.clear()
            buffer.putInt(numItemNodes)
            buffer.putInt(numItemNodes + math.abs(l))
            buffer.putInt(numItemNodes + math.abs(r))
            for (x <- hyperplane.floats) buffer.putFloat(x.toFloat)
            assert(buffer.remaining() == 0)
            write(buffer.array())
          case FlipNode(l, r) =>
            buffer.clear()
            buffer.putInt(numItemNodes)
            buffer.putInt(numItemNodes + math.abs(l))
            buffer.putInt(numItemNodes + math.abs(r))
            for (i <- 0 until d) buffer.putFloat(0)
            assert(buffer.remaining() == 0)
            write(buffer.array())
          case _ => assert(false)
        }
        numRootNodes += 1
      case InternalNode(l, r, hyperplane) =>
        assert(numRootNodes == 0)
        buffer.clear()
        buffer.putInt(Int.MaxValue) // fake
        buffer.putInt(numItemNodes + math.abs(l))
        buffer.putInt(numItemNodes + math.abs(r))
        for (x <- hyperplane.floats) buffer.putFloat(x.toFloat)
        assert(buffer.remaining() == 0)
        write(buffer.array())
        numHyperplaneNodes += 1
      case LeafNode(children: Array[Int]) =>
        assert(numRootNodes == 0)
        buffer.clear()
        buffer.putInt(children.length)
        children foreach buffer.putInt // if exceed, exception raised
        while (buffer.remaining() > 0) buffer.putInt(0) // fill 0s for safety
        assert(buffer.remaining() == 0)
        write(buffer.array())
        numLeafNodes += 1
      case FlipNode(l, r) =>
        buffer.clear()
        buffer.putInt(numItemNodes)
        buffer.putInt(numItemNodes + math.abs(l))
        buffer.putInt(numItemNodes + math.abs(r))
        for (i <- 0 until d) buffer.putFloat(0)
        assert(buffer.remaining() == 0)
        write(buffer.array())
        numHyperplaneNodes += 1
    }
    AnnoyUtilNodeStat(offset, numRootNodes, numHyperplaneNodes, numLeafNodes, numBytesWritten)
  }

  def saveItems[T <: HasId with HasVector](partId: Int, items: Iterator[T], d: Int, os: OutputStream): AnnoyUtilItemStat = {
    val buffer = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)
    val hole =  new Array[Byte](12 + d * 4)
    var numBytesWritten = 0L
    val write = { b: Array[Byte] =>
      os.write(b)
      numBytesWritten += b.length
    }

    var firstId = -1
    var numHoles = 0
    var numItems = 0
    var lastId = -1
    for (item <- items) {
      val id = item.getId
      val v = item.getVector
      if (firstId == -1) firstId = item.getId
      if (partId > 0 && numItems == 0 && numItems < id) numItems = id
      require(lastId == -1 || lastId < id, "items are not sorted")
      while (numItems < id) {
        write(hole)
        numHoles += 1
        numItems += 1
      }
      val nrm2 = Vectors.nrm2(v).toFloat
      buffer.clear()
      buffer.putInt(1)
      buffer.putFloat(nrm2 * nrm2) // Annoy stores nrm2^2
      buffer.putInt(0)
      for (x <- v.floats) buffer.putFloat(x)
      assert(buffer.remaining() == 0)
      write(buffer.array())
      numItems += 1
      lastId = id
    }
    AnnoyUtilItemStat(Seq(PartInfo(partId, firstId, lastId)), numItems, numHoles, numBytesWritten)
  }

  def dump[T <: HasId with HasVector](d: Int, sortedItemIterator: Iterator[T], nodes: Nodes, os: OutputStream): (AnnoyUtilItemStat, AnnoyUtilNodeStat) = {
    val itemStat = saveItems(0, sortedItemIterator, d, os)
    val nodeStat = saveNodes(nodes, d, itemStat.numItems, os)
    (itemStat, nodeStat)
  }

}
