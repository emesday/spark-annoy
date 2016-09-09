package annoy4s

import scala.collection.mutable.ArrayBuffer

trait Distance {
  val name: String
  def distance(x: Array[Float], y: Array[Float]): Float
  def createSplit(nodes: ArrayBuffer[Node], f: Int, rand: Random, n: Node): Unit
  def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean
  def margin(n: Node, y: Array[Float], buffer: Array[Float]): Float
  def normalizeDistance(distance: Float): Float
}
