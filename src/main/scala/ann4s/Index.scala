package ann4s

import java.io.BufferedOutputStream
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.ArrayBuffer
import scala.tools.nsc.interpreter.OutputStream
import scala.util.Random

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


  def writeAnnoyBinary[T <: HasId with HasVector](sortedItems: IndexedSeq[T], os: OutputStream): Unit = {

    val d = sortedItems.head.getVector.size
    val bos = new BufferedOutputStream(os, 1024 * 1024)
    val buffer = ByteBuffer.allocate(12 + d * 4).order(ByteOrder.LITTLE_ENDIAN)
    val hole =  new Array[Byte](12 + d * 4)

    println(s"number of nodes ${nodes.length}")

    var numHoles = 0
    var i = 0
    var lastId = -1
    for (item <- sortedItems) {
      val id = item.getId
      val v = item.getVector

      assert(lastId == -1 || lastId < id, "items are not sorted")

      while (i < id) {
        bos.write(hole)
        numHoles += 1
        i += 1
      }
      val nrm2 = Vectors.nrm2(v).toFloat
      buffer.clear()
      buffer.putInt(1)
      buffer.putFloat(nrm2 * nrm2) // Annoy stores nrm2^2
      buffer.putInt(0)
      for (x <- v.values) buffer.putFloat(x.toFloat)
      assert(buffer.remaining() == 0)
      bos.write(buffer.array())
      i += 1
      lastId = id
    }

    println(s"number of holes: $numHoles")

    val numItemNodes = i
    var numRootNodes = 0
    var numHyperplaneNodes = 0
    var numLeafNodes = 0
    nodes foreach {
      case RootNode(location) =>
        assert(numItemNodes > 0 && numHyperplaneNodes > 0 && numLeafNodes > 0)
        nodes(location) match {
          case HyperplaneNode(hyperplane, l, r) =>
            buffer.clear()
            buffer.putInt(numItemNodes)
            buffer.putInt(numItemNodes + l)
            buffer.putInt(numItemNodes + r)
            for (x <- hyperplane.values) buffer.putFloat(x.toFloat)
            assert(buffer.remaining() == 0)
            bos.write(buffer.array())
          case FlipNode(l, r) =>
            buffer.clear()
            buffer.putInt(numItemNodes)
            buffer.putInt(numItemNodes + l)
            buffer.putInt(numItemNodes + r)
            for (i <- 0 until d) buffer.putFloat(0)
            assert(buffer.remaining() == 0)
            bos.write(buffer.array())
          case _ => assert(false)
        }
        numRootNodes += 1
      case HyperplaneNode(hyperplane, l, r) =>
        assert(numRootNodes == 0)
        buffer.clear()
        buffer.putInt(Int.MaxValue) // fake
        buffer.putInt(numItemNodes + l)
        buffer.putInt(numItemNodes + r)
        for (x <- hyperplane.values) buffer.putFloat(x.toFloat)
        assert(buffer.remaining() == 0)
        bos.write(buffer.array())
        numHyperplaneNodes += 1
      case LeafNode(children: Array[Int]) =>
        assert(numRootNodes == 0)
        buffer.clear()
        buffer.putInt(children.length)
        children foreach buffer.putInt // if exceed, exception raised
        while (buffer.remaining() > 0) buffer.putInt(0) // fill 0s for safety
        assert(buffer.remaining() == 0)
        bos.write(buffer.array())
        numLeafNodes += 1
      case FlipNode(l, r) =>
        buffer.clear()
        buffer.putInt(numItemNodes)
        buffer.putInt(numItemNodes + l)
        buffer.putInt(numItemNodes + r)
        for (i <- 0 until d) buffer.putFloat(0)
        assert(buffer.remaining() == 0)
        bos.write(buffer.array())
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

