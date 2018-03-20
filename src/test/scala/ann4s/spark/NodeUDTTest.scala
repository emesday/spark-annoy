package ann4s.spark

import ann4s._
import org.apache.spark.ml.nn.NodeUDT
import org.scalatest.Matchers._
import org.scalatest._

import scala.util.Random

class NodeUDTTest extends FunSuite {

  test("") {

    val d = 64
    val random = new Random(0x0816)
    val udt = new NodeUDT

    val f16 = Vector16(Array.fill(d)(random.nextInt(Short.MaxValue).toShort))
    val f32 = Vector32(Array.fill(d)(random.nextFloat()))
    val f64 = Vector64(Array.fill(d)(random.nextDouble()))

    val f16h = HyperplaneNode(f16, 1, 2)
    val f32h = HyperplaneNode(f32, 1, 2)
    val f64h = HyperplaneNode(f64, 1, 2)

    udt.deserialize(udt.serialize(f16h)) should be (f16h)
    udt.deserialize(udt.serialize(f32h)) should be (f32h)
    udt.deserialize(udt.serialize(f64h)) should be (f64h)
  }

}
