package annoy4s

abstract class NodeStorage(dim: Int, io: NodeSerde) {

  final val nodeSizeInBytes = io.nodeSizeInBytes(dim)

  final val childrenCapacity = io.childrenCapacity(dim)

  val bufferType: String

  def ensureSize(n: Int, verbose: Boolean): Int

  def apply(i: Int): Node

  def newNode: Node

  def numNodes: Int

  def flip(): Unit

}
