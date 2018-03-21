package org.apache.spark.ml.nn

import ann4s._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.types._

class NodeUDT extends UserDefinedType[Node] {

  override def sqlType: DataType = _sqlType

  override def serialize(obj: Node): InternalRow = {
    val row = new GenericInternalRow(6)
    row.setNullAt(2)
    row.setNullAt(3)
    row.setNullAt(4)
    row.setNullAt(5)

    obj match {
      case RootNode(location) =>
        row.setByte(0, 1)
        row.update(1, UnsafeArrayData.fromPrimitiveArray(Array(location)))
      case LeafNode(children) =>
        row.setByte(0, 2)
        row.update(1, UnsafeArrayData.fromPrimitiveArray(children))
      case FlipNode(l, r) =>
        row.setByte(0, 3)
        row.update(1, UnsafeArrayData.fromPrimitiveArray(Array(l, r)))
      case HyperplaneNode(l, r, hyperplane) =>
        row.update(1, UnsafeArrayData.fromPrimitiveArray(Array(l, r)))
        hyperplane match {
          case Vector0 =>
            row.setByte(0, 100)
          case Vector8(values, w, b) =>
            row.setByte(0, 101)
            row.update(2, UnsafeArrayData.fromPrimitiveArray(values))
            row.update(4, UnsafeArrayData.fromPrimitiveArray(Array(w, b)))
          case Vector16(values) =>
            row.setByte(0, 102)
            row.update(3, UnsafeArrayData.fromPrimitiveArray(values))
          case Vector32(values) =>
            row.setByte(0, 103)
            row.update(4, UnsafeArrayData.fromPrimitiveArray(values))
          case Vector64(values) =>
            row.setByte(0, 104)
            row.update(5, UnsafeArrayData.fromPrimitiveArray(values))
        }
    }
    row
  }

  override def deserialize(datum: Any): Node = {
    datum match {
      case row: InternalRow =>
        require(row.numFields == 6,
          s"NodeUDT.deserialize given row with length ${row.numFields} but requires length == 6")
        val tpe = row.getByte(0)
        tpe match {
          case 1 =>
            val location = row.getArray(1).toIntArray()(0)
            RootNode(location)
          case 2 =>
            val children = row.getArray(1).toIntArray()
            LeafNode(children)
          case 3 =>
            val ar = row.getArray(1).toIntArray()
            FlipNode(ar(0), ar(1))
          case 100 =>
            val ar = row.getArray(1).toIntArray()
            HyperplaneNode(ar(0), ar(1), Vector0)
          case 101 =>
            val ar = row.getArray(1).toIntArray()
            val wb = row.getArray(4).toFloatArray()
            val vector = Vector8(row.getArray(2).toByteArray(), wb(0), wb(1))
            HyperplaneNode(ar(0), ar(1), vector)
          case 102 =>
            val ar = row.getArray(1).toIntArray()
            val vector = Vector16(row.getArray(3).toShortArray())
            HyperplaneNode(ar(0), ar(1), vector)
          case 103 =>
            val ar = row.getArray(1).toIntArray()
            val vector = Vector32(row.getArray(4).toFloatArray())
            HyperplaneNode(ar(0), ar(1), vector)
          case 104 =>
            val ar = row.getArray(1).toIntArray()
            val vector = Vector64(row.getArray(5).toDoubleArray())
            HyperplaneNode(ar(0), ar(1), vector)
        }
    }
  }

  override def userClass: Class[Node] = classOf[Node]

  override def equals(o: Any): Boolean = {
    o match {
      case _: NodeUDT => true
      case _ => false
    }
  }

  override def hashCode(): Int = classOf[NodeUDT].getName.hashCode

  override def typeName: String = "nnvector"

  private[spark] override def asNullable: NodeUDT = this

  private[this] val _sqlType = {
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("childrean", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("fixed8", ArrayType(ByteType, containsNull = false), nullable = true),
      StructField("fixed16", ArrayType(ShortType, containsNull = false), nullable = true),
      StructField("float32", ArrayType(FloatType, containsNull = false), nullable = true),
      StructField("float64", ArrayType(DoubleType, containsNull = false), nullable = true)))
  }
}
