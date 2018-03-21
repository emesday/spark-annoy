package ann4s

import org.scalatest.Matchers._
import org.scalatest._

class IndexAggregatorTest extends FunSuite {

  val hyperplane = Vector64(Array.emptyDoubleArray)
  val lLeaf = Array(0, 1)
  val rLeaf = Array(2, 3)
  val items = Array(
    IdVector(0, Vector64(Array(0.1))),
    IdVector(1, Vector64(Array(0.2))),
    IdVector(2, Vector64(Array(0.3))),
    IdVector(3, Vector64(Array(0.4))))

  def getAggregatedResult: IndexAggregator = {
    val aggregator = new IndexAggregator
    val nodes = Array( HyperplaneNode(1, 2, hyperplane), LeafNode(lLeaf), LeafNode(rLeaf), RootNode(0) )
    aggregator.aggregate(nodes).aggregate(nodes).aggregate(nodes)
  }

  test("IndexAggregator: result") {
    val actual = getAggregatedResult.result()
    val expected = Array(
      HyperplaneNode(1, 2, hyperplane), // 0
      LeafNode(lLeaf),                  // 1
      LeafNode(rLeaf),                  // 2
      HyperplaneNode(4, 5, hyperplane), // 3
      LeafNode(lLeaf),                  // 4
      LeafNode(rLeaf),                  // 5
      HyperplaneNode(7, 8, hyperplane), // 6
      LeafNode(lLeaf),                  // 7
      LeafNode(rLeaf),                  // 8
      RootNode(0),
      RootNode(3),
      RootNode(6)
    )

    actual.nodes should be (expected)
  }

}
