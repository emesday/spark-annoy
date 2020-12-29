package sparkannoy

import scala.util.Random

object Side extends Enumeration {

  val Left, Right = Value

  def side(b: Boolean): Value = if (!b) Left else Right

}

trait Distance {

  def distance(a: VectorWithNorm, b: IdVectorWithNorm): Double

  def distance(a: IdVectorWithNorm, b: IdVectorWithNorm): Double

  def distance(a: Vector, b: Vector): Double

  def margin(m: Vector, n: Vector): Double

  def side(m: Vector, n: Vector)(implicit random: Random): Side.Value

}

object CosineDistance extends Distance {

  def distance(a: VectorWithNorm, b: IdVectorWithNorm): Double = {
    val dot = Vectors.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def distance(a: IdVectorWithNorm, b: IdVectorWithNorm): Double = {
    val dot = Vectors.dot(a.vector, b.vector)
    val norm = a.norm * b.norm
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def distance(a: Vector, b: Vector): Double = {
    val dot = Vectors.dot(a, b)
    val norm = Vectors.nrm2(a) * Vectors.nrm2(b)
    if (norm > 0) 2 - 2 * dot / norm
    else 2
  }

  def margin(m: Vector, n: Vector): Double = Vectors.dot(m, n)

  def side(m: Vector, n: Vector)(implicit random: Random): Side.Value = {
    val dot = margin(m, n)
    if (dot == 0) Side.side(random.nextBoolean())
    else if (dot > 0) Side.Right
    else Side.Left
  }
}
