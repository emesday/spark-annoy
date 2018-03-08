package ann4s

import ann4s.spark.Kiss32Random
import org.scalatest.Matchers._
import org.scalatest._

class KissRandomTest extends FunSuite {

  test("Kiss32Random") {
    val random = new Kiss32Random()

    val expectedKiss = Array(
      2079675107L, 4185567647L, 2837635843L, 1057683632L, 1715709901L,
      2813364309L, 4260296908L, 1972631519L, 843352983L, 4105814351L)

    val expectedIndex = Array(
      56, 61, 47, 85, 95,
      88, 23, 9, 65, 6)

    val expectedFlip = Array(
      1, 1, 0, 1, 1, 1, 0, 1, 1, 1,
      1, 0, 0, 1, 0, 0, 0, 1, 0, 1,
      0, 1, 0, 0, 0, 0, 0, 1, 0, 1,
      1, 1, 0, 0, 1, 0, 1, 0, 1, 1,
      0, 1, 1, 1, 1, 1, 0, 0, 0, 0,
      0, 1, 1, 1, 0, 1, 1, 1, 0, 0,
      1, 1, 1, 1, 0, 0, 1, 0, 0, 0,
      0, 1, 1, 0, 1, 0, 1, 1, 0, 1,
      1, 0, 1, 1, 1, 0, 1, 0, 1, 1,
      0, 0, 1, 1, 1, 0, 1, 1, 1, 1).map(_ == 1)

    val actualKiss = Array.fill(expectedKiss.length)(random.kiss())
    val actualIndex = Array.fill(expectedIndex.length)(random.nextInt(100))
    val actualFlip = Array.fill(expectedFlip.length)(random.nextBoolean())

    actualKiss should be (expectedKiss)
    actualIndex should be (expectedIndex)
    actualFlip should be (expectedFlip)
  }

}
