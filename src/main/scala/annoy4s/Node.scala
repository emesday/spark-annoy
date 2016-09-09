package annoy4s

import java.nio.ByteBuffer

case class Node(dim: Int, nodeSizeInBytes: Int, underlying: ByteBuffer, offsetInBytes: Int, io: NodeIO, readonly: Boolean) {

  def getNDescendants: Int =
    io.getNDescendants(underlying, offsetInBytes)

  def getChildren(i: Int): Int =
    io.getChildren(underlying, offsetInBytes, i)

  def getAllChildren(dst: Array[Int]): Array[Int] =
    io.getAllChildren(underlying, offsetInBytes, dst)

  def getVector(dst: Array[Float]): Array[Float] =
    io.getV(underlying, offsetInBytes, dst)

  def setValue(v: Float): Unit = {
    require(!readonly)
    io.setValue(underlying, offsetInBytes, v, dim)
  }

  def setNDescendants(nDescendants: Int) = {
    require(!readonly)
    io.setNDescendants(underlying, offsetInBytes, nDescendants)
  }

  def setChildren(i: Int, v: Int): Unit = {
    require(!readonly)
    io.setChildren(underlying, offsetInBytes, i, v)
  }

  def setAllChildren(indices: Array[Int]): Unit = {
    require(!readonly)
    io.setAllChildren(underlying, offsetInBytes, indices)
  }

  def setV(v: Array[Float]): Unit = {
    require(!readonly)
    io.setV(underlying, offsetInBytes, v)
  }

  def setA(a: Float): Unit = {
    require(!readonly)
    io.setA(underlying, offsetInBytes, a)
  }

  def copyFrom(other: Node): Unit = {
    require(!readonly)
    io.copy(other.underlying, other.offsetInBytes, underlying, offsetInBytes, nodeSizeInBytes)
  }
}
