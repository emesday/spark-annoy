package ann4s

import java.nio.ByteBuffer
import java.util

import com.github.fommil.netlib.BLAS

trait Vector extends Serializable {

  def size: Int

  def numBytes: Int

  def floats: Array[Float]

  def apply(i: Int): Float

  override def equals(other: Any): Boolean = {
    other match {
      case o: Vector =>
        if (this.size != o.size) return false
        (this, o) match {
          //case (Vector0, Vector0) => true // TODO
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
  override def numBytes: Int = 1
  override def floats: Array[Float] = Array.emptyFloatArray
  override def apply(i: Int): Float = ???
}

case class Vector8(vector: Array[Byte], w: Float, b: Float) extends Vector {
  override def size: Int = vector.length
  override def numBytes: Int = 5 + vector.length + 8
  override def floats: Array[Float] = ???
  override def apply(i: Int): Float = ???
}

case class Vector16(vector: Array[Short]) extends Vector {
  override def size: Int = vector.length
  override def numBytes: Int = 5 + vector.length * 2
  override def floats: Array[Float] = vector.map { v => v.toFloat / (1 << 8) }
  override def apply(i: Int): Float = vector(i).toFloat / (1 << 8)
}

case class Vector32(vector: Array[Float]) extends Vector {
  override def size: Int = vector.length
  override def numBytes: Int = 5 + vector.length * 4
  override def floats: Array[Float] = vector
  override def apply(i: Int): Float = vector(i)
}

case class Vector64(vector: Array[Double]) extends Vector {
  override def size: Int = vector.length
  override def numBytes: Int = 5 + vector.length * 8
  override def floats: Array[Float] = vector.map(_.toFloat)
  override def apply(i: Int): Float = vector(i).toFloat
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
      case v16: Vector16 =>
        math.sqrt(dot(v16, v16))
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
      case (Vector32(sx), Vector64(dy)) =>
        var d = 0.0; var i = 0; while (i < sx.length) { d += sx(i) * dy(i); i += 1 }; d
      case (Vector64(dx), Vector32(sy)) =>
        var d = 0.0; var i = 0; while (i < dx.length) { d += dx(i) * sy(i); i += 1 }; d
      case (hx: Vector16, hy: Vector16) =>
        var d = 0.0; var i = 0; while (i < hx.size) { d += hx(i) * hy(i); i += 1 }; d
    }
  }

  def getBytes(v: Vector): Array[Byte] = {
    val bb = ByteBuffer.allocate(v.numBytes)
    fillByteBuffer(v, bb)
    bb.array()
  }

  def fillByteBuffer(v: Vector, bb: ByteBuffer): Unit = {
    v match {
      case Vector0 =>
        bb.put(1.toByte)
      case Vector8(vector, w, b) =>
        bb.put(2.toByte)
        bb.putInt(vector.length)
        bb.put(vector)
        bb.putFloat(w)
        bb.putFloat(b)
      case Vector16(vector) =>
        bb.put(3.toByte)
        bb.putInt(vector.length)
        vector foreach bb.putShort
      case Vector32(vector) =>
        bb.put(4.toByte)
        bb.putInt(vector.length)
        vector foreach bb.putFloat
      case Vector64(vector) =>
        bb.put(5.toByte)
        bb.putInt(vector.length)
        vector foreach bb.putDouble
    }
  }

  def fromByteBuffer(bb: ByteBuffer): Vector = {
    bb.get().toInt match {
      case 1 =>
        Vector0
      case 2 =>
        Vector8(Array.fill(bb.getInt)(bb.get), bb.getFloat, bb.getFloat)
      case 3 =>
        Vector16(Array.fill(bb.getInt)(bb.getShort))
      case 4 =>
        Vector32(Array.fill(bb.getInt)(bb.getFloat))
      case 5 =>
        Vector64(Array.fill(bb.getInt)(bb.getDouble))
    }
  }

  def vector16(ar: Array[Float]): Vector = {
    val quantized = ar.map { x => math.round(x * (1 << 8)).toShort }
    Vector16(quantized)
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
