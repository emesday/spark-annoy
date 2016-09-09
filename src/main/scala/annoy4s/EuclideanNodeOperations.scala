package annoy4s

import java.nio.ByteBuffer

object EuclideanNodeOperations extends EuclideanNodeOperations

/**
  * n_descendants: Int = 4
  * a: Float = 4
  * n_children[0]: Int = 4
  * n_children[1]: Int = 4
  * v: Array[Float] = f * 4
  */
trait EuclideanNodeOperations extends NodeOperations {
  override def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int): Int = ???

  override def setValue(underlying: ByteBuffer, offsetInBytes: Int, v: Float, dim: Float): Unit = ???

  override def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float] = ???

  override def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit = ???

  override def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit = ???

  override def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit = ???

  override def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit = ???

  override def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int] = ???

  override def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit = ???

  override def getChildren(underlying: ByteBuffer, offsetInByte: Int, i: Int): Int = ???
}
