package annoy4s

import java.nio.ByteBuffer

object AngularNodeOperations extends AngularNodeOperations

/**
  * n_descendants: Int = 4
  * n_children[0]: Int = 4
  * n_children[1]: Int = 4
  * v: Array[Float] = f * 4
  */
trait AngularNodeOperations extends NodeOperations {

  def nodeSizeInBytes(dim: Int): Int = 12 + dim * 4
  def childrenCapacity(dim: Int): Int = 2 + dim

  override def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int): Int = {
    underlying.position(offsetInBytes)
    underlying.getInt()
  }

  override def getChildren(underlying: ByteBuffer, offsetInByte: Int, i: Int): Int = {
    underlying.position(offsetInByte + 4 * (i + 1))
    underlying.getInt()
  }

  override def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int] = {
    underlying.position(offsetInByte + 4)
    underlying.asIntBuffer().get(dst)
    dst
  }

  override def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float] = {
    underlying.position(offsetInBytes + 12)
    underlying.asFloatBuffer().get(dst)
    dst
  }

  override def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit = {
    underlying.position(offsetInBytes)
    underlying.putInt(nDescendants)
  }

  override def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit = {
    underlying.position(offsetInBytes + 4 * (i + 1))
    underlying.putInt(v)
  }

  override def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit = {
    underlying.position(offsetInBytes + 4)
    underlying.asIntBuffer().put(indices)
    val out = getAllChildren(underlying, offsetInBytes, new Array[Int](indices.length))
    val in = indices
  }

  override def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit = {
    underlying.position(offsetInBytes + 12)
    underlying.asFloatBuffer().put(v)
  }

  override def setValue(underlying: ByteBuffer, offsetInBytes: Int, v: Float, dim: Float): Unit = {
    underlying.position(offsetInBytes + 12)
    val floatBuffer = underlying.asFloatBuffer()
    var i = 0
    while (i < dim) {
      floatBuffer.put(i, v)
      i += 1
    }
  }

  override def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit = {
    val dup = src.duplicate()
    dup.position(srcOffsetInBytes)
    dup.limit(srcOffsetInBytes + nodeSizeInBytes)

    dst.position(dstOffsetInBytes)
    dst.put(dup)
  }

}

