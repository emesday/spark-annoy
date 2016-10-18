package ann4s

import java.util.concurrent.atomic.AtomicInteger

import ann4s.Functions._
import org.apache.spark.proxy.BoundedPriorityQueue

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AnnoyIndex(dim: Int, metric: Metric, random: Random, dbPath: String) {

  val helper = new RocksDBHelper(dbPath)

  def this(f: Int, random: Random, dbPath: String) = this(f, Angular, random, dbPath)

  def this(f: Int, metric: Metric, dbPath: String) = this(f, metric, RandRandom, dbPath)

  def this(f: Int, dbPath: String) = this(f, Angular, RandRandom, dbPath)

  private val childrenCapacity: Int = 2 + dim

  private val roots = new ArrayBuffer[Int]()

  private val atomicIndex = new AtomicInteger(helper.getNumItems)

  private val atomicNodeIndex = new AtomicInteger()

  private var nItems = atomicIndex.get()

  println(s"current atomicIndex: $atomicIndex")

  reinitialize()

  def getDim: Int = dim

  def addItem(id: String, w: Array[Float], metadata: String): Unit = {
    if (!helper.exists(id))
      helper.putAll(id, atomicIndex.getAndIncrement(), w, metadata)
  }

  def build(q: Int): Unit = {
    require(q > 0)
    nItems = helper.getNumItems
    atomicNodeIndex.set(nItems)

    val indices = new ArrayBuffer[Int] ++= (0 until nItems)

    (0 until q) foreach { _ =>
      val root = makeTree(indices)
      helper.putRoot(root)
      roots += root
      showUpdate(s"pass ${roots.size}...\n")
    }
  }

  private def makeTree(indices: ArrayBuffer[Int]): Int = {
    if (indices.length == 1)
      return indices(0)

    if (indices.length <= childrenCapacity) {
      val newNodeIndex = atomicNodeIndex.getAndIncrement()
      helper.putLeafNode(newNodeIndex, indices.toArray)
      return newNodeIndex
    }

    val hyperplane = metric.createSplit(indices, dim, random, helper)

    val childrenIndices = Array.fill(2) {
      new ArrayBuffer[Int](indices.length)
    }

    val vectorBuffer = new Array[Float](dim)
    indices.foreach { case i =>
      val side = if (metric.side(hyperplane, helper.getFeat(i, vectorBuffer), random)) 1 else 0
      childrenIndices(side) += i
    }

    // If we didn't find a hyperplane, just randomize sides as a last option
    while (childrenIndices(0).isEmpty || childrenIndices(1).isEmpty) {
      if (indices.length > 100000)
        showUpdate("Failed splitting %d items\n", indices.length)

      childrenIndices(0).clear()
      childrenIndices(1).clear()

      indices.foreach { i =>
        // Just randomize...
        childrenIndices(if (random.flip()) 1 else 0) += i
      }
    }

    val flip = if (childrenIndices(0).length > childrenIndices(1).length) 1 else 0
    val children = new Array[Int](2)
    children(0 ^ flip) = makeTree(childrenIndices(0 ^ flip))
    children(1 ^ flip) = makeTree(childrenIndices(1 ^ flip))

    val newNodeIndex = atomicNodeIndex.getAndIncrement()
    helper.putHyperplaneNode(newNodeIndex, children, hyperplane)
    newNodeIndex
  }

  def close(): Unit = {
    helper.close()
    showUpdate("closed\n")
  }

  def getNItems: Int = helper.getNumItems

  def getNnsByVector(w: Array[Float], n: Int): Array[(Int, Float)] = getNnsByVector(w, n, -1)

  def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = getAllNns(w, n, k)

  val ord = new Ordering[(Int, Float)]{
    def compare(x: (Int, Float), y: (Int, Float)): Int = {
      val compare1 = Ordering[Float].compare(x._2, y._2)
      if (compare1 != 0) return compare1
      val compare2 = Ordering[Int].compare(x._1, y._1)
      if (compare2 != 0) return compare2
      0
    }
  }

  private def getAllNns(v: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    val vectorBuffer = new Array[Float](dim)
    val searchK = if (k == -1) n * roots.length else k

    val q = new mutable.PriorityQueue[(Float, Int)] ++= roots.map(Float.PositiveInfinity -> _)

    var searched = 0
    val nns = new ArrayBuffer[Int](searchK)
    while (searched < searchK && q.nonEmpty) {
      val (d, i) = q.dequeue()
      if (i < nItems) {
        nns += i
        searched += 1
      } else {
        val (children, hyperplane) = helper.getNode(i, dim)
        if (hyperplane == null) {
          nns ++= children
          searched += children.length
        } else {
          val margin = metric.margin(hyperplane, v)
          q += math.min(d, +margin) -> children(1)
          q += math.min(d, -margin) -> children(0)
        }
      }
    }

    val boundedQueue = new BoundedPriorityQueue[(Int, Float)](n)(ord.reverse)
    val seen = new mutable.BitSet
    for (j <- nns) {
      if (!seen(j)) {
        boundedQueue += j -> metric.distance(v, helper.getFeat(j, vectorBuffer))
        seen += j
      }
    }

    val result = boundedQueue.toArray
    var i = 0
    while (i < result.length) {
      result(i) = (result(i)._1, metric.normalizeDistance(result(i)._2))
      i += 1
    }
    java.util.Arrays.sort(result, 0, result.length, ord)
    result
  }

  private def reinitialize(): Unit = {
    nItems = 0
    roots.clear()
    atomicIndex.set(0)
  }

}

