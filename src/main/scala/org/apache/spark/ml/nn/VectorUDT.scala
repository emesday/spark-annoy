package org.apache.spark.ml.nn

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  GenericInternalRow,
  UnsafeArrayData
}
import org.apache.spark.sql.types._
import sparkannoy.{Vector0, Vector16, Vector32, Vector64, Vector8}

class VectorUDT extends UserDefinedType[sparkannoy.Vector] {

  override def sqlType: DataType = _sqlType

  override def serialize(obj: sparkannoy.Vector): InternalRow = {
    val row = new GenericInternalRow(5)
    row.setNullAt(1)
    row.setNullAt(2)
    row.setNullAt(3)
    row.setNullAt(4)
    obj match {
      case Vector0 =>
        row.setByte(0, 0)
      case Vector8(values, w, b) =>
        row.setByte(0, 1)
        row.update(1, UnsafeArrayData.fromPrimitiveArray(values))
        row.update(3, UnsafeArrayData.fromPrimitiveArray(Array(w, b)))
      case Vector16(values) =>
        row.setByte(0, 2)
        row.update(2, UnsafeArrayData.fromPrimitiveArray(values))
      case Vector32(values) =>
        row.setByte(0, 3)
        row.update(3, UnsafeArrayData.fromPrimitiveArray(values))
      case Vector64(values) =>
        row.setByte(0, 4)
        row.update(4, UnsafeArrayData.fromPrimitiveArray(values))
    }
    row
  }

  override def deserialize(datum: Any): sparkannoy.Vector = {
    datum match {
      case row: InternalRow =>
        require(
          row.numFields == 5,
          s"nn.VectorUDT.deserialize given row with length ${row.numFields}" +
            " but requires length == 5")
        val tpe = row.getByte(0)
        tpe match {
          case 0 =>
            Vector0
          case 1 =>
            val wb = row.getArray(3).toFloatArray()
            Vector8(row.getArray(1).toByteArray(), wb(0), wb(1))
          case 2 =>
            Vector16(row.getArray(2).toShortArray())
          case 3 =>
            Vector32(row.getArray(3).toFloatArray())
          case 4 =>
            Vector64(row.getArray(4).toDoubleArray())
        }
    }
  }

  override def userClass: Class[sparkannoy.Vector] = classOf[sparkannoy.Vector]

  override def equals(o: Any): Boolean = {
    o match {
      case _: VectorUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[VectorUDT].getName.hashCode

  override def typeName: String = "nn.vector"

  private[spark] override def asNullable: VectorUDT = this

  private[this] val _sqlType = {
    StructType(
      Seq(
        StructField("type", ByteType, nullable = false),
        StructField("fixed8",
                    ArrayType(ByteType, containsNull = false),
                    nullable = true),
        StructField("fixed16",
                    ArrayType(ShortType, containsNull = false),
                    nullable = true),
        StructField("float32",
                    ArrayType(FloatType, containsNull = false),
                    nullable = true),
        StructField("float64",
                    ArrayType(DoubleType, containsNull = false),
                    nullable = true)
      ))
  }
}

object VectorUDT {

  def register(): Unit = {
    UDTRegistration.register("ann4s.Vector", "org.apache.spark.ml.nn.VectorUDT")
    UDTRegistration.register("ann4s.EmptyVector",
                             "org.apache.spark.ml.nn.VectorUDT")
    UDTRegistration.register("ann4s.Fixed8Vector",
                             "org.apache.spark.ml.nn.VectorUDT")
    UDTRegistration.register("ann4s.Fixed16Vector",
                             "org.apache.spark.ml.nn.VectorUDT")
    UDTRegistration.register("ann4s.Float32Vector",
                             "org.apache.spark.ml.nn.VectorUDT")
    UDTRegistration.register("ann4s.Float64Vector",
                             "org.apache.spark.ml.nn.VectorUDT")
  }

}
