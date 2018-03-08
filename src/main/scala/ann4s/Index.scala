package ann4s

import java.io.BufferedOutputStream
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.interpreter.OutputStream
import scala.util.Random

class Index(val nodes: IndexedSeq[Node], val withItems: Boolean) extends Serializable {

  def this(nodes: IndexedSeq[Node]) = this(nodes, false)

  val roots: Array[RootNode] = {
    val roots = new ArrayBuffer[RootNode]()
    var i = nodes.length - 1
    while (0 <= i && nodes(i).isInstanceOf[RootNode]) {
      roots += nodes(i).asInstanceOf[RootNode]
      i -= 1
    }
    roots.reverse.toArray
  }

  def writeAnnoyBinary(d: Int, os: OutputStream): Unit = {
    assert(withItems, "index should include items for Annoy")

    val bos = new BufferedOutputStream(os, 1024 * 1024)

    val bf = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)

    println(s"number of nodes ${nodes.length}")

    var numItemNodes = 0
    var numRootNodes = 0
    var numHyperplaneNodes = 0
    var numLeafNodes = 0
    nodes foreach {
      case ItemNode(vector) =>
        assert(numRootNodes == 0 && numHyperplaneNodes == 0 && numLeafNodes == 0)
        val nrm2 = Vectors.nrm2(vector).toFloat
        bf.clear()
        bf.putInt(1)
        bf.putFloat(nrm2 * nrm2) // Annoy stores nrm2^2
        bf.putInt(0)
        for (x <- vector.values) bf.putFloat(x.toFloat)
        assert(bf.remaining() == 0)
        bos.write(bf.array())
        numItemNodes += 1
      case RootNode(location) =>
        assert(numItemNodes > 0 && numHyperplaneNodes > 0 && numLeafNodes > 0)
        nodes(location) match {
          case HyperplaneNode(hyperplane, l, r) =>
            bf.clear()
            bf.putInt(numItemNodes)
            bf.putInt(l)
            bf.putInt(r)
            for (x <- hyperplane.values) bf.putFloat(x.toFloat)
            assert(bf.remaining() == 0)
            bos.write(bf.array())
          case FlipNode(l, r) =>
            bf.clear()
            bf.putInt(numItemNodes)
            bf.putInt(l)
            bf.putInt(r)
            for (i <- 0 until d) bf.putFloat(0)
            assert(bf.remaining() == 0)
            bos.write(bf.array())
          case _ => assert(false)
        }
        numRootNodes += 1
      case HyperplaneNode(hyperplane, l, r) =>
        assert(numRootNodes == 0)
        bf.clear()
        bf.putInt(Int.MaxValue) // fake
        bf.putInt(l)
        bf.putInt(r)
        for (x <- hyperplane.values) bf.putFloat(x.toFloat)
        assert(bf.remaining() == 0)
        bos.write(bf.array())
        numHyperplaneNodes += 1
      case LeafNode(children: Array[Int]) =>
        assert(numRootNodes == 0)
        bf.clear()
        bf.putInt(children.length)
        children foreach bf.putInt // if exceed, exception raised
        while (bf.remaining() > 0) bf.putInt(0) // fill 0s for safety
        assert(bf.remaining() == 0)
        bos.write(bf.array())
        numLeafNodes += 1
      case FlipNode(l, r) =>
        bf.clear()
        bf.putInt(numItemNodes)
        bf.putInt(l)
        bf.putInt(r)
        for (i <- 0 until d) bf.putFloat(0)
        assert(bf.remaining() == 0)
        bos.write(bf.array())
        numHyperplaneNodes += 1
    }
    bos.flush()
    println(numItemNodes, numRootNodes, numHyperplaneNodes, numLeafNodes)
  }

  def toStructuredNodes: StructuredNodes = {
    StructuredNodes(nodes.map(_.toStructuredNode))
  }

  def getCandidates(v: IdVectorWithNorm): Array[Int] = ???

  def traverse(vector: Vector)(implicit distance: Distance, random: Random): Int = {
    var nodeId = roots(0).location
    var node = nodes(nodeId)
    while (!node.isInstanceOf[LeafNode] && !node.isInstanceOf[FlipNode]) {
      node match {
        case HyperplaneNode(hyperplane, l, r) =>
          if (distance.side(hyperplane, vector) == Side.Left) nodeId = l
          else nodeId = r
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

      var l = -1
      var r = -1
      if (leftChildren.length <= rightChildren.length) {
        l = recurse(leftChildren, nodes)
        r = recurse(rightChildren, nodes)
      } else {
        r = recurse(rightChildren, nodes)
        l = recurse(leftChildren, nodes)
      }

      if (failed) {
        nodes += FlipNode(l, r)
      } else {
        nodes += HyperplaneNode(hyperplane, l, r)
      }
      nodes.length - 1
    }
  }

}

