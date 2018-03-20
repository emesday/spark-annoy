package ann4s

trait Node extends Serializable {

  def toStructuredNode: StructuredNode

}

case class RootNode(location: Int) extends Node {
  override def toStructuredNode: StructuredNode = {
    StructuredNode(1, location, -1, Array.emptyDoubleArray, Array.emptyIntArray)
  }
}

case class HyperplaneNode(hyperplane: Vector, l: Int, r: Int) extends Node {
  override def toStructuredNode: StructuredNode = {
    StructuredNode(2, l, r, hyperplane.values, Array.emptyIntArray)
  }
}

case class LeafNode(children: Array[Int]) extends Node {
  override def toStructuredNode: StructuredNode = {
    StructuredNode(3, -1, -1, Array.emptyDoubleArray, children)
  }
}

case class FlipNode(l: Int, r: Int) extends Node {
  override def toStructuredNode: StructuredNode = {
    StructuredNode(4, l, r, Array.emptyDoubleArray, Array.emptyIntArray)
  }
}

// TODO: UDT
case class StructuredNode(nodeType: Int, l: Int, r: Int, hyperplane: Array[Double], children: Array[Int]) {

  def toNode: Node = {
    nodeType match {
      case 1 => RootNode(l)
      case 2 => HyperplaneNode(DVector(hyperplane), l, r)
      case 3 => LeafNode(children)
      case 4 => FlipNode(l, r)
    }
  }

}

case class StructuredNodes(nodes: IndexedSeq[StructuredNode]) {

  def copyCosineForest(): Index = {
    new Index(nodes.map(_.toNode))
  }
}

