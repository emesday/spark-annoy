package ann4s

trait Node extends Serializable

case class RootNode(location: Int) extends Node

case class HyperplaneNode(hyperplane: Vector, l: Int, r: Int) extends Node

case class LeafNode(children: Array[Int]) extends Node

case class FlipNode(l: Int, r: Int) extends Node


