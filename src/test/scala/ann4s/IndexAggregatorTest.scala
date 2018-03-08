package ann4s

import org.scalatest.Matchers._
import org.scalatest._

class IndexAggregatorTest extends FunSuite {

  val hyperplane = DVector(Array.emptyDoubleArray)
  val lLeaf = Array(0, 1)
  val rLeaf = Array(2, 3)
  val items = Array(
    IdVector(0, DVector(Array(0.1))),
    IdVector(1, DVector(Array(0.2))),
    IdVector(2, DVector(Array(0.3))),
    IdVector(3, DVector(Array(0.4))))

  def getAggregatedResult: IndexAggregator = {
    val aggregator = new IndexAggregator
    val nodes = Array( HyperplaneNode(hyperplane, 1, 2), LeafNode(lLeaf), LeafNode(rLeaf), RootNode(0) )
    aggregator.aggregate(nodes).aggregate(nodes).aggregate(nodes)
  }

  test("IndexAggregator: result") {
    val actual = getAggregatedResult.result()
    val expected = Array(
      HyperplaneNode(hyperplane, 1, 2), // 0
      LeafNode(lLeaf),                  // 1
      LeafNode(rLeaf),                  // 2
      HyperplaneNode(hyperplane, 4, 5), // 3
      LeafNode(lLeaf),                  // 4
      LeafNode(rLeaf),                  // 5
      HyperplaneNode(hyperplane, 7, 8), // 6
      LeafNode(lLeaf),                  // 7
      LeafNode(rLeaf),                  // 8
      RootNode(0),
      RootNode(3),
      RootNode(6)
    )

    actual.nodes should be (expected)
    actual.withItems should be (false)
  }

  test("IndexAggregator: result with prependItems") {
    val actual = getAggregatedResult.prependItems(items).result()
    val expected = Array(
      ItemNode(items(0).vector),          // 0
      ItemNode(items(1).vector),          // 1
      ItemNode(items(2).vector),          // 2
      ItemNode(items(3).vector),          // 3
      HyperplaneNode(hyperplane, 5, 6),   // 4
      LeafNode(lLeaf),                    // 5
      LeafNode(rLeaf),                    // 6
      HyperplaneNode(hyperplane, 8, 9),   // 7
      LeafNode(lLeaf),                    // 8
      LeafNode(rLeaf),                    // 9
      HyperplaneNode(hyperplane, 11, 12), // 10
      LeafNode(lLeaf),                    // 11
      LeafNode(rLeaf),                    // 12
      RootNode(4),
      RootNode(7),
      RootNode(10)
    )

    actual.nodes should be (expected)
    actual.withItems should be (true)
  }

}
