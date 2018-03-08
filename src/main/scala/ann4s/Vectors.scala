package ann4s

import com.github.fommil.netlib.BLAS

trait Vector {

  def size: Int

  def values: Array[Double]

}

case class SVector(vector: Array[Float]) extends Vector {
  override def size: Int = vector.length
  override def values: Array[Double] = vector.map(_.toDouble) // TODO
}

case class DVector(vector: Array[Double]) extends Vector {
  override def size: Int = vector.length
  override def values: Array[Double] = vector
}

object Vectors {

  private val blas: BLAS = BLAS.getInstance()

  def scal(da: Double, x: Vector): Unit = {
    x match {
      case SVector(sx) =>
        blas.sscal(sx.length, da.toFloat, sx, 1)
      case DVector(dx) =>
        blas.dscal(dx.length, da, dx, 1)
    }
  }

  def axpy(da: Double, x: Vector, y: Vector): Unit = {
    (x, y) match {
      case (SVector(sx), SVector(sy)) =>
        blas.saxpy(sx.length, da.toFloat, sx, 1, sy, 1)
      case (DVector(dx), DVector(dy)) =>
        blas.daxpy(dx.length, da, dx, 1, dy, 1)
    }
  }

  def nrm2(x: Vector): Double  = {
    x match {
      case SVector(sx) =>
        blas.snrm2(sx.length, sx, 1).toDouble
      case DVector(dx) =>
        blas.dnrm2(dx.length, dx, 1)
    }
  }

  def dot(x: Vector, y: Vector): Double = {
    (x, y) match {
      case (SVector(sx), SVector(sy)) =>
        blas.sdot(sx.length, sx, 1, sy, 1).toDouble
      case (DVector(dx), DVector(dy)) =>
        blas.ddot(dx.length, dx, 1, dy, 1)
    }
  }

}

trait HasId {
  def getId: Int
}

trait HasVector {
  def getVector: Vector
}

trait HasNorm {
  def getNorm: Double
}

case class IdVector(id: Int, vector: Vector) extends HasId with HasVector {
  def toIdVectorWithNorm: IdVectorWithNorm = IdVectorWithNorm(id, vector)

  override def getId: Int = id
  override def getVector: Vector = vector
}

case class VectorWithNorm(vector: Vector, var norm: Double)
  extends HasVector with HasNorm {

  def aggregate(other: IdVectorWithNorm, c: Int): this.type = {
    Vectors.scal(c, vector)
    Vectors.axpy(1.0 / other.norm, other.vector, vector)
    Vectors.scal(1.0 / (c + 1), vector)
    norm = Vectors.nrm2(vector)
    this
  }

  override def getVector: Vector = vector
  override def getNorm: Double = norm
}

object VectorWithNorm {

  def apply(vector: Vector): VectorWithNorm =
    VectorWithNorm(vector, Vectors.nrm2(vector))

}

case class IdVectorWithNorm(id: Int, vector: Vector, norm: Double)
  extends HasId with HasVector with HasNorm {

  def copyVectorWithNorm: VectorWithNorm = {
    val nrm2 = Vectors.nrm2(vector)
    vector match {
      case SVector(sx) =>
        val copied = SVector(sx.clone())
        Vectors.scal(1 / nrm2, copied)
        VectorWithNorm(copied, 1)
      case DVector(dx) =>
        val copied = DVector(dx.clone())
        Vectors.scal(1 / nrm2, copied)
        VectorWithNorm(copied, 1)
    }
  }

  override def getId: Int = id
  override def getVector: Vector = vector
  override def getNorm: Double = norm
}

object IdVectorWithNorm {

  def apply(id: Int, vector: Vector): IdVectorWithNorm =
    IdVectorWithNorm(id, vector, Vectors.nrm2(vector))

  def apply(id: Int, vector: Array[Float]): IdVectorWithNorm =
    IdVectorWithNorm(id, SVector(vector))

  def apply(id: Int, vector: Array[Double]): IdVectorWithNorm =
    IdVectorWithNorm(id, DVector(vector))

}
