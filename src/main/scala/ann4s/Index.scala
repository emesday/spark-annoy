package ann4s

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class Nodes(nodes: IndexedSeq[Node]) {
  def toIndex: Index = new Index(nodes)
}

class Index(val nodes: IndexedSeq[Node]) extends Serializable {

  val roots: Array[RootNode] = {
    val roots = new ArrayBuffer[RootNode]()
    var i = nodes.length - 1
    while (0 <= i && nodes(i).isInstanceOf[RootNode]) {
      roots += nodes(i).asInstanceOf[RootNode]
      i -= 1
    }
    roots.reverse.toArray
  }

  def getNodes: Nodes = Nodes(nodes)

  def getCandidates(v: IdVectorWithNorm): Array[Int] = ???

  def traverse(vector: Vector)(implicit distance: Distance, random: Random): Int = {
    var nodeId = roots(0).location
    var node = nodes(nodeId)
    while (!node.isInstanceOf[LeafNode] && !node.isInstanceOf[FlipNode]) {
      node match {
        case InternalNode(l, r, hyperplane) =>
          if (distance.side(hyperplane, vector) == Side.Left) nodeId = math.abs(l)
          else nodeId = math.abs(r)
        case _ => assert(false)
      }
      node = nodes(nodeId)
    }
    nodeId
  }

}

object IndexBuilder {

  val iterationSteps = 200

  def twoMeans(points: IndexedSeq[IdVectorWithNorm])(implicit distance: Distance, random: Random): (Vector, Vector) = {
    val count = points.length
    val i = random.nextInt(count)
    var j = random.nextInt(count - 1)
    j += (if (j >= i) 1 else 0)

    val p = points(i).copyVectorWithNorm
    val q = points(j).copyVectorWithNorm
    var ic = 1
    var jc = 1

    Iterator.fill(iterationSteps)(random.nextInt(points.length))
      .foreach { k =>
        val kp = points(k)
        if (kp.norm > 0) {
          val di = ic * distance.distance(p, kp)
          val dj = jc * distance.distance(q, kp)

          if (di < dj) {
            p.aggregate(kp, ic)
            ic += 1
          } else {
            q.aggregate(kp, jc)
            jc += 1
          }
        }
      }

    (p.vector, q.vector)
  }

  def createSplit(sample: IndexedSeq[IdVectorWithNorm])(implicit distance: Distance, random: Random): Vector = {
    val (p, q) = twoMeans(sample)
    Vectors.axpy(-1, q, p)
    val norm = Vectors.nrm2(p)
    Vectors.scal(1 / norm, p)
    p
  }
}

class IndexBuilder(numTrees: Int, leafNodeCapacity: Int)(implicit  distance: Distance, random: Random) extends Serializable {

  assert(numTrees > 0)
  assert(leafNodeCapacity > 1)

  import IndexBuilder._

  def build(points: IndexedSeq[IdVectorWithNorm]): Index = {
    val nodes = new ArrayBuffer[Node]()
    val roots = new ArrayBuffer[RootNode]()
    0 until numTrees foreach { _ =>
      val rootId = recurse(points, nodes)
      roots += RootNode(rootId)
    }
    nodes ++= roots
    new Index(nodes)
  }

  def recurse(points: IndexedSeq[IdVectorWithNorm], nodes: ArrayBuffer[Node]): Int = {
    if (points.length <= leafNodeCapacity) {
      nodes += LeafNode(points.map(_.id).toArray)
      nodes.length - 1
    } else {
      var failed = false
      val hyperplane = createSplit(points)
      val leftChildren = new ArrayBuffer[IdVectorWithNorm]
      val rightChildren = new ArrayBuffer[IdVectorWithNorm]
      points foreach { p =>
        distance.side(hyperplane, p.vector) match {
          case Side.Left  => leftChildren += p
          case Side.Right => rightChildren += p
        }
      }

      if (leftChildren.isEmpty || rightChildren.isEmpty) {
        failed = true
        leftChildren.clear()
        rightChildren.clear()
        // BLAS.scal(0, hyperplane) // set zeros
        points foreach { p =>
          Side.side(random.nextBoolean()) match {
            case Side.Left => leftChildren += p
            case Side.Right => rightChildren += p
          }
        }
      }

      var (l, r) = if (leftChildren.length <= rightChildren.length) {
        val l = recurse(leftChildren, nodes)
        val r = recurse(rightChildren, nodes)
        (l, r)
      } else {
        val r = recurse(rightChildren, nodes)
        val l = recurse(leftChildren, nodes)
        (l, r)
      }

      // trick for check if the children of hyperplane node are LeafNode or not.
      if (nodes(l).isInstanceOf[LeafNode]) l = -l
      if (nodes(r).isInstanceOf[LeafNode]) r = -r

      if (failed) {
        nodes += FlipNode(l, r)
      } else {
        nodes += InternalNode(l, r, hyperplane)
      }
      nodes.length - 1
    }
  }

}

