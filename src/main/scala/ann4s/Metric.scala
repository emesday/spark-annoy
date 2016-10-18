package ann4s

import scala.collection.mutable.ArrayBuffer

trait Metric {

  val name: String

  def distance(x: Array[Float], y: Array[Float]): Float

  def createSplit(nodes: ArrayBuffer[Int], f: Int, rand: Random, helper: RocksDBHelper): Array[Float]

  def side(n: Array[Float], y: Array[Float], random: Random): Boolean

  def margin(n: Array[Float], y: Array[Float]): Float

  def normalizeDistance(distance: Float): Float

}
