package ann4s

import scala.collection.mutable.ArrayBuffer

object Functions {

  val Zero = 0f

  val One = 1f

  val iterationSteps = 200

  def showUpdate(text: String, xs: Any*): Unit = Console.err.print(text.format(xs: _*))

  def getNorm(x: Array[Float]): Float = {
    var sqNorm: Double = 0
    var z = 0
    while (z < x.length) {
      sqNorm += x(z) * x(z)
      z += 1
    }
    math.sqrt(sqNorm).toFloat
  }

  def normalize(sx: Array[Float]): Unit = {
    val norm = getNorm(sx)
    var z = 0
    while (z < sx.length) {
      sx(z) /= norm
      z += 1
    }
  }

  def twoMeans(nodes: ArrayBuffer[Int], cosine: Boolean, iv: Array[Float], jv: Array[Float], metric: Metric, rand: Random, helper: RocksDBHelper): Unit = {
    val count = nodes.length
    val dim = iv.length

    val i = rand.index(count)
    var j = rand.index(count - 1)
    j += (if (j >= i) 1 else 0)

    helper.getFeat(i, iv)
    helper.getFeat(j, jv)

    if (cosine) {
      normalize(iv)
      normalize(jv)
    }

    var ic = 1
    var jc = 1
    var l = 0
    val vectorBuffer = new Array[Float](dim)
    while (l < iterationSteps) {
      val k = rand.index(count)
      helper.getFeat(k, vectorBuffer)
      val zz = vectorBuffer
      val di = ic * metric.distance(iv, zz)
      val dj = jc * metric.distance(jv, zz)
      val norm = if (cosine) getNorm(zz) else One
      if (di < dj) {
        var z = 0
        while (z < dim) {
          iv(z) = (iv(z) * ic + zz(z) / norm) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        var z = 0
        while (z < dim) {
          jv(z) = (jv(z) * jc + zz(z) / norm) / (jc + 1)
          z += 1
        }
        jc += 1
      }
      l += 1
    }
  }
}

