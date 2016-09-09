package annoy4s

abstract class NodeContainer {
  val bufferType: String
  def ensureSize(n: Int, verbose: Boolean): Int
  def apply(i: Int): Node
  def newNode: Node
  def getSize: Int
  def flip(): Unit
}
