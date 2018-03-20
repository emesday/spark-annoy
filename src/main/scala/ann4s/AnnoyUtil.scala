package ann4s

import java.io.{BufferedOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}

object AnnoyUtil {

  def dump[T <: HasId with HasVector](sortedItems: IndexedSeq[T], nodes: Nodes, os: OutputStream): Unit = {

    val d = sortedItems.head.getVector.size
    val bos = new BufferedOutputStream(os, 1024 * 1024)
    val buffer = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)
    val hole =  new Array[Byte](12 + d * 4)

    println(s"number of nodes ${nodes.nodes.length}")

    var numHoles = 0
    var i = 0
    var lastId = -1
    for (item <- sortedItems) {
      val id = item.getId
      val v = item.getVector

      assert(lastId == -1 || lastId < id, "items are not sorted")

      while (i < id) {
        bos.write(hole)
        numHoles += 1
        i += 1
      }
      val nrm2 = Vectors.nrm2(v).toFloat
      buffer.clear()
      buffer.putInt(1)
      buffer.putFloat(nrm2 * nrm2) // Annoy stores nrm2^2
      buffer.putInt(0)
      for (x <- v.floats) buffer.putFloat(x.toFloat)
      assert(buffer.remaining() == 0)
      bos.write(buffer.array())
      i += 1
      lastId = id
    }

    println(s"number of holes: $numHoles")

    val numItemNodes = i
    var numRootNodes = 0
    var numHyperplaneNodes = 0
    var numLeafNodes = 0
    nodes.nodes foreach {
      case RootNode(location) =>
        assert(numItemNodes > 0 && numHyperplaneNodes > 0 && numLeafNodes > 0)
        nodes.nodes(location) match {
          case HyperplaneNode(hyperplane, l, r) =>
            buffer.clear()
            buffer.putInt(numItemNodes)
            buffer.putInt(numItemNodes + l)
            buffer.putInt(numItemNodes + r)
            for (x <- hyperplane.floats) buffer.putFloat(x.toFloat)
            assert(buffer.remaining() == 0)
            bos.write(buffer.array())
          case FlipNode(l, r) =>
            buffer.clear()
            buffer.putInt(numItemNodes)
            buffer.putInt(numItemNodes + l)
            buffer.putInt(numItemNodes + r)
            for (i <- 0 until d) buffer.putFloat(0)
            assert(buffer.remaining() == 0)
            bos.write(buffer.array())
          case _ => assert(false)
        }
        numRootNodes += 1
      case HyperplaneNode(hyperplane, l, r) =>
        assert(numRootNodes == 0)
        buffer.clear()
        buffer.putInt(Int.MaxValue) // fake
        buffer.putInt(numItemNodes + l)
        buffer.putInt(numItemNodes + r)
        for (x <- hyperplane.floats) buffer.putFloat(x.toFloat)
        assert(buffer.remaining() == 0)
        bos.write(buffer.array())
        numHyperplaneNodes += 1
      case LeafNode(children: Array[Int]) =>
        assert(numRootNodes == 0)
        buffer.clear()
        buffer.putInt(children.length)
        children foreach buffer.putInt // if exceed, exception raised
        while (buffer.remaining() > 0) buffer.putInt(0) // fill 0s for safety
        assert(buffer.remaining() == 0)
        bos.write(buffer.array())
        numLeafNodes += 1
      case FlipNode(l, r) =>
        buffer.clear()
        buffer.putInt(numItemNodes)
        buffer.putInt(numItemNodes + l)
        buffer.putInt(numItemNodes + r)
        for (i <- 0 until d) buffer.putFloat(0)
        assert(buffer.remaining() == 0)
        bos.write(buffer.array())
        numHyperplaneNodes += 1
    }
    bos.flush()
    println(numItemNodes, numRootNodes, numHyperplaneNodes, numLeafNodes)
  }

}
