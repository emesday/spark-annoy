package annoy4s

import java.nio.ByteBuffer

case class Node(dim: Int, nodeSizeInBytes: Int, underlying: ByteBuffer, offsetInBytes: Int, ops: AngularNodeOperations, readonly: Boolean) {

  def getNDescendants: Int =
    ops.getNDescendants(underlying, offsetInBytes)

  def getChildren(i: Int): Int =
    ops.getChildren(underlying, offsetInBytes, i)

  def getAllChildren(dst: Array[Int]): Array[Int] =
    ops.getAllChildren(underlying, offsetInBytes, dst)

  def getVector(dst: Array[Float]): Array[Float] =
    ops.getV(underlying, offsetInBytes, dst)

  def setValue(v: Float): Unit = {
    require(!readonly)
    ops.setValue(underlying, offsetInBytes, v, dim)
  }

  def setNDescendants(nDescendants: Int) = {
    require(!readonly)
    ops.setNDescendants(underlying, offsetInBytes, nDescendants)
  }

  def setChildren(i: Int, v: Int): Unit = {
    require(!readonly)
    ops.setChildren(underlying, offsetInBytes, i, v)
  }

  def setAllChildren(indices: Array[Int]): Unit = {
    require(!readonly)
    ops.setAllChildren(underlying, offsetInBytes, indices)
  }

  def setV(v: Array[Float]): Unit = {
    require(!readonly)
    ops.setV(underlying, offsetInBytes, v)
  }

  def copyFrom(other: Node): Unit = {
    require(!readonly)
    ops.copy(other.underlying, other.offsetInBytes, underlying, offsetInBytes, nodeSizeInBytes)
  }
}
