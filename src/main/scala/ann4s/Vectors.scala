package ann4s

import java.util

import com.github.fommil.netlib.BLAS

trait Vector extends Serializable {

  def size: Int

  def floats: Array[Float]

  override def equals(other: Any): Boolean = {
    other match {
      case o: Vector =>
        if (this.size != o.size) return false
        (this, o) match {
          case (Vector0, Vector0) => true
          case (a: Vector8, b: Vector8) =>
            if (a.w != b.w) false
            else if (a.b != b.b) false
            else util.Arrays.equals(a.vector, b.vector)
          case (a: Vector16, b: Vector16) =>
            util.Arrays.equals(a.vector, b.vector)
          case (a: Vector32, b: Vector32) =>
            util.Arrays.equals(a.vector, b.vector)
          case (a: Vector64, b: Vector64) =>
            util.Arrays.equals(a.vector, b.vector)
          case (_, _) => false
        }
      case _ => false
    }
  }

}

case object Vector0 extends Vector {
  override def size: Int = 0
  override def floats: Array[Float] = Array.emptyFloatArray
}

case class Vector8(vector: Array[Byte], w: Float, b: Float) extends Vector {
  override def size: Int = vector.length
  override def floats: Array[Float] = ???
}

case class Vector16(vector: Array[Short]) extends Vector {
  override def size: Int = vector.length
  override def floats: Array[Float] = ???
}

case class Vector32(vector: Array[Float]) extends Vector {
  override def size: Int = vector.length
  override def floats: Array[Float] = vector
}

case class Vector64(vector: Array[Double]) extends Vector {
  override def size: Int = vector.length
  override def floats: Array[Float] = vector.map(_.toFloat)
}

object Vectors {

  private val blas: BLAS = BLAS.getInstance()

  def scal(da: Double, x: Vector): Unit = {
    x match {
      case Vector32(sx) =>
        blas.sscal(sx.length, da.toFloat, sx, 1)
      case Vector64(dx) =>
        blas.dscal(dx.length, da, dx, 1)
    }
  }

  def axpy(da: Double, x: Vector, y: Vector): Unit = {
    (x, y) match {
      case (Vector32(sx), Vector32(sy)) =>
        blas.saxpy(sx.length, da.toFloat, sx, 1, sy, 1)
      case (Vector64(dx), Vector64(dy)) =>
        blas.daxpy(dx.length, da, dx, 1, dy, 1)
    }
  }

  def nrm2(x: Vector): Double  = {
    x match {
      case Vector32(sx) =>
        blas.snrm2(sx.length, sx, 1).toDouble
      case Vector64(dx) =>
        blas.dnrm2(dx.length, dx, 1)
    }
  }

  def dot(x: Vector, y: Vector): Double = {
    (x, y) match {
      case (Vector32(sx), Vector32(sy)) =>
        blas.sdot(sx.length, sx, 1, sy, 1).toDouble
      case (Vector64(dx), Vector64(dy)) =>
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
      case Vector32(sx) =>
        val copied = Vector32(sx.clone())
        Vectors.scal(1 / nrm2, copied)
        VectorWithNorm(copied, 1)
      case Vector64(dx) =>
        val copied = Vector64(dx.clone())
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
    IdVectorWithNorm(id, Vector32(vector))

  def apply(id: Int, vector: Array[Double]): IdVectorWithNorm =
    IdVectorWithNorm(id, Vector64(vector))

}
