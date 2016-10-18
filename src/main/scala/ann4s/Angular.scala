package ann4s

import scala.collection.mutable.ArrayBuffer

object Angular extends Metric with AngularNodeStruct {

  import Functions._

  override val name = "angular"

  override def distance(x: Array[Float], y: Array[Float]): Float = {
    require(x.length == y.length)
    var pp = 0f
    var qq = 0f
    var pq = 0f
    var z = 0
    while (z < x.length) {
      pp += x(z) * x(z)
      qq += y(z) * y(z)
      pq += x(z) * y(z)
      z += 1
    }
    val ppqq = pp * qq
    if (ppqq > 0) (2.0 - 2.0 * pq / Math.sqrt(ppqq)).toFloat else 2.0f
  }

  override def margin(n: Node, sx: Array[Float], buffer: Array[Float]): Float = {
    val sy = n.getVector(buffer)
    var dot: Float = 0
    var z = 0
    while (z < sx.length) {
      dot += sx(z) * sy(z)
      z += 1
    }
    dot
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
    twoMeans(nodes, true, bestIv, bestJv, this, rand)

    val result = bestIv
    var z = 0
    while (z < dim) {
      result(z) = bestIv(z) - bestJv(z)
      z += 1
    }

    normalize(result)
    n.setVector(result)
  }

  override def normalizeDistance(distance: Float): Float = {
    math.sqrt(math.max(distance, Zero)).toFloat
  }

}
