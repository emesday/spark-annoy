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

    nodes(subTreeId) = subTreeRoot.withOffset(offset)

    nodes.indices foreach { i =>
      nodes(i) match {
        case n@InternalNode(l, _, _) if -l == subTreeId => nodes(i) = n.copy(l = -l)
        case n@InternalNode(_, r, _) if -r == subTreeId => nodes(i) = n.copy(r = -r)
        case n@FlipNode(l, _) if -l == subTreeId => nodes(i) = n.copy(l = -l)
        case n@FlipNode(_, r) if -r == subTreeId => nodes(i) = n.copy(r = -r)
        case _ =>
      }
    }

    other.dropRight(1) foreach {
      case root: RootNode =>
        roots += root.withOffset(offset)
      case node =>
        nodes += node.withOffset(offset)
    }
    nodes ++= roots
    this
  }

  def result(): Index = new Index(nodes)

}
