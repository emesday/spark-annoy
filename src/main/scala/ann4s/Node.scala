package ann4s

import java.nio.ByteBuffer

case class Node(
  dim: Int,
  nodeSizeInBytes: Int,
  underlying: ByteBuffer,
  offsetInBytes: Int,
  struct: NodeStruct,
  readonly: Boolean = false) {

  def getNDescendants: Int = {
    underlying.position(offsetInBytes + struct.offsetDescendants)
    underlying.getInt()
  }

  def getChildren(i: Int): Int = {
    underlying.position(offsetInBytes + struct.offsetChildren + 4 * i)
    underlying.getInt()
  }

  def getAllChildren(dst: Array[Int]): Array[Int] = {
    underlying.position(offsetInBytes + struct.offsetChildren)
    underlying.asIntBuffer().get(dst)
    dst
  }

  def getVector(dst: Array[Float]): Array[Float] = {
    underlying.position(offsetInBytes + struct.offsetValue)
    var i = 0
    while (i < dst.length) {
      dst(i) = underlying.getFloat
      i += 1
    }
    dst
  }

  def getA: Float = {
    underlying.position(offsetInBytes + struct.offsetA)
    underlying.getFloat()
  }

  def setValue(v: Float): Unit = {
    require(!readonly)
    underlying.position(offsetInBytes + struct.offsetValue)
    var i = 0
    while (i < dim) {
      underlying.putFloat(v)
      i += 1
    }
  }

  def setNDescendants(nDescendants: Int): Unit = {
    require(!readonly)
    underlying.position(offsetInBytes + struct.offsetDescendants)
    underlying.putInt(nDescendants)
  }

  def setChildren(i: Int, v: Int): Unit = {
    require(!readonly)
    underlying.position(offsetInBytes + struct.offsetChildren + 4 * i)
    underlying.putInt(v)
  }

  def setAllChildren(indices: Array[Int]): Unit = {
    require(!readonly)
    underlying.position(offsetInBytes + struct.offsetChildren)
    underlying.asIntBuffer().put(indices)
  }

  def setVector(v: Array[Float]): Unit = {
    require(!readonly)
    underlying.position(offsetInBytes + struct.offsetValue)
    var i = 0
    while (i < v.length) {
      underlying.putFloat(v(i))
      i += 1
    }
  }

  def setA(a: Float): Unit = {
    require(!readonly)
    underlying.position(offsetInBytes + struct.offsetA)
    underlying.putFloat(a)
  }

  def copyFrom(other: Node): Unit = {
    require(!readonly)
    _copy(other.underlying, other.offsetInBytes, underlying, offsetInBytes, nodeSizeInBytes)
  }

  private def _copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit = {
    val dup = src.duplicate()
    dup.position(srcOffsetInBytes)
    dup.limit(srcOffsetInBytes + nodeSizeInBytes)

    dst.position(dstOffsetInBytes)
    dst.put(dup)
  }

  def commit() = {
    require(!readonly)
    struct.commit(underlying, offsetInBytes)
  }

}
