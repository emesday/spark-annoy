package ann4s

import scala.collection.mutable.ArrayBuffer

object Euclidean extends Metric with EuclideanNodeStruct {

  import Functions._

  override val name = "euclidean"

  override def distance(x: Array[Float], y: Array[Float]): Float = {
    require(x.length == y.length)
    var i = 0
    var d: Float = 0f
    var t: Float = 0f
    while (i < x.length) {
      t = x(i) - y(i)
      d += t * t
      i += 1
    }
    d
  }

  override def margin(n: Node, sx: Array[Float], buffer: Array[Float]): Float ={
    val sy = n.getVector(buffer)
    var dot: Float = 0
    var z = 0
    while (z < sx.length) {
      dot += sx(z) * sy(z)
      z += 1
    }
    dot + n.getA
  }

  override def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean = {
    val dot = margin(n, y, buffer)
    if (dot != Zero) {
      dot > 0
    } else {
      random.flip()
    }
  }

  override def createSplit(nodes: ArrayBuffer[Node], dim: Int, rand: Random, n: Node): Unit = {
    val bestIv = new Array[Float](dim)
    val bestJv = new Array[Float](dim)
    twoMeans(nodes, false, bestIv, bestJv, this, rand)

    val vectorBuffer = n.getVector(new Array[Float](dim))
    var z = 0
    while (z < dim) {
      vectorBuffer(z) = bestIv(z) - bestJv(z)
      z += 1
    }
    normalize(vectorBuffer)
    var a = 0f
    z = 0
    while (z < dim) {
      a += -vectorBuffer(z) * (bestIv(z) + bestJv(z)) / 2
      z += 1
    }
    n.setVector(vectorBuffer)
    n.setA(a)
  }

  override def normalizeDistance(distance: Float): Float = {
    math.sqrt(math.max(distance, Zero)).toFloat
  }
}
