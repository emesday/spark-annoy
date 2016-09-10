package annoy4s

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

trait Metric extends Distance with NodeSerde

trait Distance {

  val name: String

  def distance(x: Array[Float], y: Array[Float]): Float

  def createSplit(nodes: ArrayBuffer[Node], f: Int, rand: Random, n: Node): Unit

  def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean

  def margin(n: Node, y: Array[Float], buffer: Array[Float]): Float

  def normalizeDistance(distance: Float): Float

}

trait NodeSerde {

  def nodeSizeInBytes(dim: Int): Int

  def childrenCapacity(dim: Int): Int

  val offsetDescendants: Int

  val offsetChildren: Int

  val offsetValue: Int

  final def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int) : Int = {
    underlying.position(offsetInBytes + offsetDescendants)
    underlying.getInt()
  }

  final def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit = {
    underlying.position(offsetInBytes + offsetDescendants)
    underlying.putInt(nDescendants)
  }

  final def getChildren(underlying: ByteBuffer, offsetInBytes: Int,  i: Int): Int = {
    underlying.position(offsetInBytes + offsetChildren + 4 * i)
    underlying.getInt()
  }

  final def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit = {
    underlying.position(offsetInBytes + offsetChildren + 4 * i)
    underlying.putInt(v)
  }

  final def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int] = {
    underlying.position(offsetInByte + offsetChildren)
    underlying.asIntBuffer().get(dst)
    dst
  }

  final def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit = {
    underlying.position(offsetInBytes + offsetChildren)
    underlying.asIntBuffer().put(indices)
  }

  final def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float] = {
    underlying.position(offsetInBytes + offsetValue)
    underlying.asFloatBuffer().get(dst)
    dst
  }

  final def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit = {
    underlying.position(offsetInBytes + offsetValue)
    underlying.asFloatBuffer().put(v)
  }

  final def setValue(underlying: ByteBuffer, offsetInBytes: Int, v: Float, dim: Float): Unit = {
    underlying.position(offsetInBytes + offsetValue)
    val floatBuffer = underlying.asFloatBuffer()
    var i = 0
    while (i < dim) {
      floatBuffer.put(i, v)
      i += 1
    }
  }

  final def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit = {
    val dup = src.duplicate()
    dup.position(srcOffsetInBytes)
    dup.limit(srcOffsetInBytes + nodeSizeInBytes)

    dst.position(dstOffsetInBytes)
    dst.put(dup)
  }

  def getA(underlying: ByteBuffer, offsetInBytes: Int): Float

  def setA(underlying: ByteBuffer, offsetInBytes: Int, a: Float): Unit

}

