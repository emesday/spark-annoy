package ann4s

/**
  * RootNode, HyperplaneNode, and FlipNode are loaded on memory each query server.
  * LeafNode is stored in persistent layer
  */
trait Node extends Serializable

case class RootNode(location: Int) extends Node {
  def withOffset(offset: Int): RootNode = {
    copy(if (location > 0) location + offset else location - offset)
  }
}

// l or r can be negative value when the corresponding child is LeafNode
case class HyperplaneNode(hyperplane: Vector, l: Int, r: Int) extends Node {
  def withOffset(offset: Int): HyperplaneNode = {
    val newL = if (l > 0) l + offset else l - offset
    val newR = if (r > 0) r + offset else r - offset
    copy(hyperplane, newL, newR)
  }
}

case class FlipNode(l: Int, r: Int) extends Node {
  def withOffset(offset: Int): FlipNode = {
    val newL = if (l > 0) l + offset else l - offset
    val newR = if (r > 0) r + offset else r - offset
    copy(newL, newR)
  }
}

case class LeafNode(children: Array[Int]) extends Node


