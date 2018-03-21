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
        roots += root.withOffset(offset)
      case hyperplane: InternalNode =>
        nodes += hyperplane.withOffset(offset)
      case flip: FlipNode =>
        nodes += flip.withOffset(offset)
      case leaf: LeafNode =>
        nodes += leaf
    }
    nodes ++= roots
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
      case hyperplane: InternalNode =>
        hyperplane.withOffset(offset)
      case flip: FlipNode =>
        flip.withOffset(offset)
      case leaf: LeafNode =>
        leaf
    }

    other.dropRight(1) foreach {
      case root: RootNode =>
        roots += root.withOffset(offset)
      case hyperplane: InternalNode =>
        nodes += hyperplane.withOffset(offset)
      case flip: FlipNode =>
        nodes += flip.withOffset(offset)
      case leaf: LeafNode =>
        nodes += leaf
    }
    nodes ++= roots
    this
  }

  def result(): Index = new Index(nodes)

}
