package ann4s

import java.nio.ByteBuffer

/**
  * RootNode, HyperplaneNode, and FlipNode are loaded on memory each query server.
  * LeafNode is stored in persistent layer
  */
trait Node extends Serializable {

  def toByteArray: Array[Byte]

  def isLeafNode: Boolean

}

object Node {

  def fromByteArray(ar: Array[Byte]): Node = {
    val bb = ByteBuffer.wrap(ar)
    bb.get.toInt match {
      case 1 => RootNode(bb.getInt)
      case 2 => InternalNode(bb.getInt, bb.getInt, Vectors.fromByteBuffer(bb))
      case 3 => FlipNode(bb.getInt, bb.getInt)
      case 4 => LeafNode(Array.fill(bb.getInt)(bb.getInt))
    }
  }

}

case class RootNode(location: Int) extends Node {
  def withOffset(offset: Int): RootNode = {
    copy(if (location > 0) location + offset else location - offset)
  }

  override def toByteArray: Array[Byte] = {
    val bb = ByteBuffer.allocate(5)
    bb.put(1.toByte)
    bb.putInt(location)
    bb.array()
  }

  def isLeafNode: Boolean = false
}

/**
  * l or r is the negative value when the corresponding child is LeafNode
  */
case class InternalNode(l: Int, r: Int, hyperplane: Vector) extends Node {
  def withOffset(offset: Int): InternalNode = {
    val newL = if (l >= 0) l + offset else l - offset
    val newR = if (r >= 0) r + offset else r - offset
    copy(newL, newR, hyperplane)
  }

  override def toByteArray: Array[Byte] = {
    val bb = ByteBuffer.allocate(9 + hyperplane.numBytes)
    bb.put(2.toByte)
    bb.putInt(l)
    bb.putInt(r)
    Vectors.fillByteBuffer(hyperplane, bb)
    bb.array()
  }

  def isLeafNode: Boolean = false
}

/**
  * a special case of InternalNode whose hyperplane is 0
  */
case class FlipNode(l: Int, r: Int) extends Node {
  def withOffset(offset: Int): FlipNode = {
    val newL = if (l >= 0) l + offset else l - offset
    val newR = if (r >= 0) r + offset else r - offset
    copy(newL, newR)
  }

  override def toByteArray: Array[Byte] = {
    val bb = ByteBuffer.allocate(9)
    bb.put(3.toByte)
    bb.putInt(l)
    bb.putInt(r)
    bb.array()
  }

  def isLeafNode: Boolean = false
}

case class LeafNode(children: Array[Int]) extends Node {

  override def toByteArray: Array[Byte] = {
    val bb = ByteBuffer.allocate(1 + 4 + children.length * 4)
    bb.put(4.toByte)
    bb.putInt(children.length)
    children foreach bb.putInt
    bb.array()
  }

  def isLeafNode: Boolean = true
}




