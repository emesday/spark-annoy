package ann4s

import scala.collection.mutable.ArrayBuffer

class IndexAggregator(var nodes: ArrayBuffer[Node]) {

  def this() = this(new ArrayBuffer[Node]())

  def aggregate(other: IndexAggregator): this.type = aggregate(other.nodes)

  def aggregate(other: IndexedSeq[Node]): this.type = {
    val roots = new ArrayBuffer[Node]()
    var i = nodes.length - 1
    while (0 <= i && nodes(i).isInstanceOf[RootNode]) {
      roots.insert(0, nodes(i))
      i -= 1
    }
    // remove roots in nodes
    nodes.reduceToSize(nodes.length - roots.length)
    nodes.sizeHint(nodes.length + other.length + roots.length)

    val offset = nodes.length
    other foreach {
      case root: RootNode =>
        roots += root.copy(root.location + offset)
      case hyperplane: HyperplaneNode =>
        nodes += hyperplane.copy(l = hyperplane.l + offset, r = hyperplane.r + offset)
      case flip: FlipNode =>
        nodes += flip.copy(l = flip.l + offset, r = flip.r + offset)
      case leaf: LeafNode =>
        nodes += leaf
    }
    nodes ++= roots
    this
  }

  def mergeSubTrees(it: Iterator[(Int, IndexedSeq[StructuredNode])]): this.type = {
    it.foreach { case (subTreeId, subTreeNodes) =>
      mergeSubTree(subTreeId, subTreeNodes.map(_.toNode))
    }
    this
  }

  def mergeSubTree(subTreeId: Int, other: IndexedSeq[Node]): this.type = {
    val subTreeRoot = other.last match {
      case RootNode(location) => other(location)
    }

    val roots = new ArrayBuffer[Node]()
    var i = nodes.length - 1
    while (0 <= i && nodes(i).isInstanceOf[RootNode]) {
      roots.insert(0, nodes(i))
      i -= 1
    }
    // remove roots in nodes
    nodes.reduceToSize(nodes.length - roots.length)
    nodes.sizeHint(nodes.length + other.length + roots.length)

    val offset = nodes.length

    nodes(subTreeId) = subTreeRoot match {
      case hyperplane: HyperplaneNode =>
        hyperplane.copy(l = hyperplane.l + offset, r = hyperplane.r + offset)
      case flip: FlipNode =>
        flip.copy(l = flip.l + offset, r = flip.r + offset)
      case leaf: LeafNode =>
        leaf
    }

    other.dropRight(1) foreach {
      case root: RootNode =>
        roots += root.copy(root.location + offset)
      case hyperplane: HyperplaneNode =>
        nodes += hyperplane.copy(l = hyperplane.l + offset, r = hyperplane.r + offset)
      case flip: FlipNode =>
        nodes += flip.copy(l = flip.l + offset, r = flip.r + offset)
      case leaf: LeafNode =>
        nodes += leaf
    }
    nodes ++= roots
    this
  }

  def result(): Index = new Index(nodes)

}
