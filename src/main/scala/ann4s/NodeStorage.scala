package ann4s

abstract class NodeStorage(dim: Int, io: NodeStruct) {

  final val nodeSizeInBytes = io.nodeSizeInBytes(dim)

  final val childrenCapacity = io.childrenCapacity(dim)

  val bufferType: String

  def ensureSize(n: Int, verbose: Boolean): Int

  def apply(i: Int): Node

  def newNode: Node

  def numNodes: Int

  def getSize: Int

  def flip(): Unit

  def close(): Unit

}
