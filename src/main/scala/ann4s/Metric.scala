package ann4s

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

trait Metric extends Distance with NodeStruct

trait Distance {

  val name: String

  def distance(x: Array[Float], y: Array[Float]): Float

  def createSplit(nodes: ArrayBuffer[Node], f: Int, rand: Random, n: Node): Unit

  def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean

  def margin(n: Node, y: Array[Float], buffer: Array[Float]): Float

  def normalizeDistance(distance: Float): Float

}

trait NodeStruct {

  def nodeSizeInBytes(dim: Int): Int

  def childrenCapacity(dim: Int): Int

  val offsetDescendants: Int

  val offsetChildren: Int

  val offsetValue: Int

  val offsetA: Int

  def commit(underlying: ByteBuffer, offsetInBytes: Int) = {}

}

