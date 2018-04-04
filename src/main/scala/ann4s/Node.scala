package ann4s

import java.nio.ByteBuffer

/**
  * RootNode, HyperplaneNode, and FlipNode are loaded on memory each query server.
  * LeafNode is stored in persistent layer
  */
trait Node extends Serializable {
  def withOffset(offset: Int): Node
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
    copy(if (location >= 0) location + offset else location - offset)
  }
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
}

case class LeafNode(children: Array[Int]) extends Node {
  def withOffset(offset: Int): LeafNode = this
}




