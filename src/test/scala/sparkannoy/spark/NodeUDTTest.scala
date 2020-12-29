package sparkannoy.spark

import org.apache.spark.ml.nn.NodeUDT
import org.scalatest._
import org.scalatest.Matchers._
import sparkannoy.{InternalNode, Vector16, Vector32, Vector64}

import scala.util.Random

class NodeUDTTest extends FunSuite {

  test("NodeUDT") {

    val d = 64
    val random = new Random(0x0816)
    val udt = new NodeUDT

    val f16 = Vector16(Array.fill(d)(random.nextInt(Short.MaxValue).toShort))
    val f32 = Vector32(Array.fill(d)(random.nextFloat()))
    val f64 = Vector64(Array.fill(d)(random.nextDouble()))

    val f16h = InternalNode(1, 2, f16)
    val f32h = InternalNode(1, 2, f32)
    val f64h = InternalNode(1, 2, f64)

    udt.deserialize(udt.serialize(f16h)) should be(f16h)
    udt.deserialize(udt.serialize(f32h)) should be(f32h)
    udt.deserialize(udt.serialize(f64h)) should be(f64h)
  }

}
