package ann4s

import java.io.{BufferedOutputStream, OutputStream}
import java.nio.{ByteBuffer, ByteOrder}

object AnnoyUtil {

  def dump[T <: HasId with HasVector](sortedItemIterator: Iterator[T], nodes: Nodes, os: OutputStream): Unit = {

    val d = nodes.nodes.find(_.isInstanceOf[InternalNode]) match {
      case Some(InternalNode(_, _, hyperplane)) => hyperplane.size
    }
    val bos = new BufferedOutputStream(os, 1024 * 1024)
    val buffer = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)
    val hole =  new Array[Byte](12 + d * 4)
    var numBytesWritten = 0L
    val write = { b: Array[Byte] =>
      bos.write(b)
      numBytesWritten += b.length
    }

    println(s"d: $d")
    println(s"number of nodes ${nodes.nodes.length}")

    var numHoles = 0
    var i = 0
    var lastId = -1
    for (item <- sortedItemIterator) {
      val id = item.getId
      val v = item.getVector

      assert(lastId == -1 || lastId < id, "items are not sorted")

      while (i < id) {
        write(hole)
        numHoles += 1
        i += 1
      }
      val nrm2 = Vectors.nrm2(v).toFloat
      buffer.clear()
      buffer.putInt(1)
      buffer.putFloat(nrm2 * nrm2) // Annoy stores nrm2^2
      buffer.putInt(0)
      for (x <- v.floats) buffer.putFloat(x)
      assert(buffer.remaining() == 0)
      write(buffer.array())
      i += 1
      lastId = id
    }

    val numBytesForItems = numBytesWritten

    println(s"number of holes: $numHoles")
    println(s"numBytes for storing items: $numBytesForItems")

    val numItemNodes = i
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
    bos.flush()

    println(s"numBytes for storing nodes: ${numBytesWritten - numBytesForItems}")
    println(numItemNodes, numRootNodes, numHyperplaneNodes, numLeafNodes)
  }

}
