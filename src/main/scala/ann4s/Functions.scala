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

  def twoMeans(nodes: ArrayBuffer[Node], cosine: Boolean, iv: Array[Float], jv: Array[Float], metric: Distance, rand: Random): Unit = {
    val count = nodes.length
    val dim = iv.length

    val i = rand.index(count)
    var j = rand.index(count - 1)
    j += (if (j >= i) 1 else 0)
    nodes(i).getVector(iv)
    nodes(j).getVector(jv)

    if (cosine) {
      normalize(iv)
      normalize(jv)
    }

    var ic = 1
    var jc = 1
    var l = 0
    var z = 0
    val v = new Array[Float](dim)
    while (l < iterationSteps) {
      val k = rand.index(count)
      val zz = nodes(k).getVector(v)
      val di = ic * metric.distance(iv, zz)
      val dj = jc * metric.distance(jv, zz)
      val norm = if (cosine) getNorm(zz) else One
      if (di < dj) {
        z = 0
        while (z < dim) {
          iv(z) = (iv(z) * ic + zz(z) / norm) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        z = 0
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

// code from https://github.com/scalanlp/breeze/blob/42c2e2522cf09259a34879e1c3b13b81176e410f/math/src/main/scala/breeze/util/TopK.scala
class TopK[T](k : Int, reversed: Boolean = false)(implicit ord : Ordering[T]) extends Iterable[T] {

  import scala.collection.JavaConversions._

  val _ord = if (reversed) ord.reverse else ord

  private val keys = new java.util.TreeSet[T](_ord)

  def +=(e : T) = {
    if (keys.size < k) {
      keys.add(e)
    } else if (keys.size > 0 && _ord.lt(keys.first, e) && !keys.contains(e)) {
      keys.remove(keys.first)
      keys.add(e)
    }
  }

  override def iterator : Iterator[T] =
    keys.descendingIterator

  override def size = keys.size

}

