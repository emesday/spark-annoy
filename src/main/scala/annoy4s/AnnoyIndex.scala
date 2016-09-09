package annoy4s

import java.io.{FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.{ByteBuffer, ByteOrder}
import java.util

import annoy4s.Functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

trait NodeOperations {

  def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int) : Int

  def getChildren(underlying: ByteBuffer, offsetInByte: Int,  i: Int): Int

  def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int]

  def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float]

  def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit

  def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit

  def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit

  def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit

  def setValue(underlying: ByteBuffer, offsetInBytes: Int, v: Float, dim: Float): Unit

  def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit

}

case class Node(dim: Int, nodeSizeInBytes: Int, underlying: ByteBuffer, offsetInBytes: Int, ops: AngularNodeOperations, readonly: Boolean) {

  def getNDescendants: Int =
    ops.getNDescendants(underlying, offsetInBytes)

  def getChildren(i: Int): Int =
    ops.getChildren(underlying, offsetInBytes, i)

  def getAllChildren(dst: Array[Int]): Array[Int] =
    ops.getAllChildren(underlying, offsetInBytes, dst)

  def getVector(dst: Array[Float]): Array[Float] =
    ops.getV(underlying, offsetInBytes, dst)

  def setValue(v: Float): Unit = {
    require(!readonly)
    ops.setValue(underlying, offsetInBytes, v, dim)
  }

  def setNDescendants(nDescendants: Int) = {
    require(!readonly)
    ops.setNDescendants(underlying, offsetInBytes, nDescendants)
  }

  def setChildren(i: Int, v: Int): Unit = {
    require(!readonly)
    ops.setChildren(underlying, offsetInBytes, i, v)
  }

  def setAllChildren(indices: Array[Int]): Unit = {
    require(!readonly)
    ops.setAllChildren(underlying, offsetInBytes, indices)
  }

  def setV(v: Array[Float]): Unit = {
    require(!readonly)
    ops.setV(underlying, offsetInBytes, v)
  }

  def copyFrom(other: Node): Unit = {
    require(!readonly)
    ops.copy(other.underlying, other.offsetInBytes, underlying, offsetInBytes, nodeSizeInBytes)
  }
}

abstract class NodeContainer {
  val bufferType: String
  def ensureSize(n: Int, verbose: Boolean): Int
  def apply(i: Int): Node
  def newNode: Node
  def getSize: Int
  def flip(): Unit
}

class MappedNodeContainer[T <: NodeOperations](dim: Int, filename: String) extends NodeContainer {

  val memoryMappedFile = new RandomAccessFile(filename, "r")
  val fileSize = memoryMappedFile.length()
  val underlying = memoryMappedFile.getChannel.map(
    FileChannel.MapMode.READ_ONLY, 0, fileSize)
    .order(ByteOrder.LITTLE_ENDIAN)

  val (nodeSizeInBytes, childrenCapacity, ops) =  {
    //    case cls if classOf[AngularNode].isAssignableFrom(cls) =>
    (AngularNodeOperations.nodeSizeInBytes(dim), AngularNodeOperations.childrenCapacity(dim), AngularNodeOperations)
  }

  override val bufferType = underlying.getClass.getSimpleName

  override def getSize: Int = fileSize.toInt

  override def ensureSize(n: Int, verbose: Boolean): Int = throw new IllegalAccessError("readonly")

  override def newNode: Node = throw new IllegalAccessError("readonly")

  override def apply(i: Int): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, ops, true)

  override def flip(): Unit = throw new IllegalAccessError("readonly")

  def close() = {
    memoryMappedFile.close()
  }
}

class HeapNodeContainer[T <: NodeOperations](dim: Int, _size: Int) extends NodeContainer {

  val reallocation_factor = 1.3

  val (nodeSizeInBytes, childrenCapacity, ops) =  {
    //    case cls if classOf[AngularNode].isAssignableFrom(cls) =>
    (AngularNodeOperations.nodeSizeInBytes(dim), AngularNodeOperations.childrenCapacity(dim), AngularNodeOperations)
  }

  var size = _size
  var underlying = ByteBuffer.allocate(nodeSizeInBytes * size).order(ByteOrder.LITTLE_ENDIAN)

  override val bufferType: String = underlying.getClass.getSimpleName

  override def getSize: Int = size

  override def ensureSize(n: Int, verbose: Boolean): Int = {
    if (n > size) {
      val newsize = math.max(n, (size + 1) * reallocation_factor).toInt
      if (verbose) showUpdate("Reallocating to %d nodes\n", newsize)
      val newBuffer: ByteBuffer = ByteBuffer.allocateDirect(nodeSizeInBytes * newsize).order(ByteOrder.LITTLE_ENDIAN)

      underlying.rewind()
      newBuffer.put(underlying)
      underlying = newBuffer
      size = newsize
    }
    size
  }

  var readonly = false

  override def apply(i: Int): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, ops, readonly)

  override def newNode: Node = Node(dim, nodeSizeInBytes, ByteBuffer.allocate(nodeSizeInBytes).order(ByteOrder.LITTLE_ENDIAN), 0, ops, readonly)

  override def flip(): Unit = underlying.flip()

  def prepareToWrite(): Unit = {
    readonly = true
    underlying.rewind()
  }
}

/**
  * n_descendants: Int = 4
  * n_children[0]: Int = 4
  * n_children[1]: Int = 4
  * v: Array[Float] = f * 4
  */
trait AngularNodeOperations extends NodeOperations {

  def nodeSizeInBytes(dim: Int): Int = 12 + dim * 4
  def childrenCapacity(dim: Int): Int = 2 + dim

  override def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int): Int = {
    underlying.position(offsetInBytes)
    underlying.getInt()
  }

  override def getChildren(underlying: ByteBuffer, offsetInByte: Int, i: Int): Int = {
    underlying.position(offsetInByte + 4 * (i + 1))
    underlying.getInt()
  }

  override def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int] = {
    underlying.position(offsetInByte + 4)
    underlying.asIntBuffer().get(dst)
    dst
  }

  override def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float] = {
    underlying.position(offsetInBytes + 12)
    underlying.asFloatBuffer().get(dst)
    dst
  }

  override def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit = {
    underlying.position(offsetInBytes)
    underlying.putInt(nDescendants)
  }

  override def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit = {
    underlying.position(offsetInBytes + 4 * (i + 1))
    underlying.putInt(v)
  }

  override def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit = {
    underlying.position(offsetInBytes + 4)
    underlying.asIntBuffer().put(indices)
    val out = getAllChildren(underlying, offsetInBytes, new Array[Int](indices.length))
    val in = indices
  }

  override def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit = {
    underlying.position(offsetInBytes + 12)
    underlying.asFloatBuffer().put(v)
  }

  override def setValue(underlying: ByteBuffer, offsetInBytes: Int, v: Float, dim: Float): Unit = {
    underlying.position(offsetInBytes + 12)
    val floatBuffer = underlying.asFloatBuffer()
    var i = 0
    while (i < dim) {
      floatBuffer.put(i, v)
      i += 1
    }
  }

  override def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit = {
    val dup = src.duplicate()
    dup.position(srcOffsetInBytes)
    dup.limit(srcOffsetInBytes + nodeSizeInBytes)

    dst.position(dstOffsetInBytes)
    dst.put(dup)
  }

}

object AngularNodeOperations extends AngularNodeOperations

/*
class EuclideanNode(f: Int) {
  // n_descendants: Int = 4
  // a: Float = 4
  // n_children[0]: Int = 4
  // n_children[1]: Int = 4
  // v: Array[Float] = f * 4
  private val underlying = new Array[Byte](16 + f * 4)
}
*/

trait Random {
  def flip(): Boolean
  def index(n: Int): Int
}

object RandRandom extends Random {
  val rnd = new scala.util.Random
  override def flip(): Boolean = rnd.nextBoolean()
  override def index(n: Int): Int = rnd.nextInt(n)
}

trait BLASInterface {
  def nrm2(x: Array[Float]): Float
  def scal(sa: Float, sx: Array[Float]): Unit
  def dot(sx: Array[Float], sy: Array[Float]): Float
}

//object NetlibBLAS extends BLASInterface {
//
//  val blas = com.github.fommil.netlib.BLAS.getInstance()
//
//  override def nrm2(x: Array[Float]): Float = blas.snrm2(x.length, x, 1)
//
//  override def scal(sa: Float, sx: Array[Float]): Unit = blas.sscal(sx.length, sa, sx, 1)
//
//  override def dot(sx: Array[Float], sy: Array[Float]): Float = blas.sdot(sx.length, sx, 1, sy, 1)
//}

object SimpleBLAS extends BLASInterface {
  override def nrm2(x: Array[Float]): Float = {
    var sqNorm: Double = 0
    var z = 0
    while (z < x.length) {
      sqNorm += x(z) * x(z)
      z += 1
    }
    math.sqrt(sqNorm).toFloat
  }

  override def scal(sa: Float, sx: Array[Float]): Unit = {
    val norm = nrm2(sx)
    var z = 0
    while (z < sx.length) {
      sx(z) /= norm
      z += 1
    }
  }

  override def dot(sx: Array[Float], sy: Array[Float]): Float = {
    var dot: Float = 0
    var z = 0
    while (z < sx.length) {
      dot += sx(z) * sy(z)
      z += 1
    }
    dot
  }
}

object Functions {
  val Zero = 0f
  val One = 1f
  val blas = SimpleBLAS
  val iterationSteps = 200

  def showUpdate(text: String, xs: Any*): Unit = Console.err.print(text.format(xs: _*))

  def getNorm(v: Array[Float]): Float = blas.nrm2(v)

  def normalize(v: Array[Float]): Unit = blas.scal(One / getNorm(v), v)

  def twoMeans(nodes: ArrayBuffer[Node], cosine: Boolean, iv: Array[Float], jv: Array[Float], metric: Distance, rand: Random): Unit = {
    val count = nodes.length
    val dim = iv.length

    val i = rand.index(count)
    var j = rand.index(count - 1)
    j += (if (j >= i) 1 else 0)
    nodes(i).getVector(iv)
    nodes(j).getVector(jv)

    if (cosine) {
      normalize(iv)
      normalize(jv)
    }

    var ic = 1
    var jc = 1
    var l = 0
    var z = 0
    val v = new Array[Float](dim)
    while (l < iterationSteps) {
      val k = rand.index(count)
      val zz = nodes(k).getVector(v)
      val di = ic * metric.distance(iv, zz)
      val dj = jc * metric.distance(jv, zz)
      val norm = if (cosine) getNorm(zz) else One
      if (di < dj) {
        z = 0
        while (z < dim) {
          iv(z) = (iv(z) * ic + zz(z) / norm) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        z = 0
        while (z < dim) {
          jv(z) = (jv(z) * jc + zz(z) / norm) / (jc + 1)
          z += 1
        }
        jc += 1
      }
      l += 1
    }
  }
}

trait Distance {
  val name: String
  def distance(x: Array[Float], y: Array[Float]): Float
  def createSplit(nodes: ArrayBuffer[Node], f: Int, rand: Random, n: Node): Unit
  def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean
  def margin(n: Node, y: Array[Float], buffer: Array[Float]): Float
  def normalizeDistance(distance: Float): Float
}

object Angular extends Distance {

  override val name = "angular"

  override def distance(x: Array[Float], y: Array[Float]): Float = {
    require(x.length == y.length)
    val pp = blas.dot(x, x)
    val qq = blas.dot(y, y)
    val pq = blas.dot(x, y)
    val ppqq: Double = pp * qq
    if (ppqq > 0) (2.0 - 2.0 * pq / Math.sqrt(ppqq)).toFloat else 2.0f
  }

  override def margin(n: Node, y: Array[Float], buffer: Array[Float]): Float = blas.dot(n.getVector(buffer), y)

  override def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean = {
    val dot = margin(n, y, buffer)
    if (dot != Zero) {
      dot > 0
    } else {
      random.flip()
    }
  }

  override def createSplit(nodes: ArrayBuffer[Node], dim: Int, rand: Random, n: Node): Unit = {
    val bestIv = new Array[Float](dim)
    val bestJv = new Array[Float](dim)
    twoMeans(nodes, true, bestIv, bestJv, this, rand)

    val vectorBuffer = n.getVector(new Array[Float](dim))
    var z = 0
    while (z < dim) {
      vectorBuffer(z) = bestIv(z) - bestJv(z)
      z += 1
    }
    normalize(vectorBuffer)
    n.setV(vectorBuffer)
  }

  override def normalizeDistance(distance: Float): Float = {
    math.sqrt(math.max(distance, Zero)).toFloat
  }
}

class AnnoyIndex(dim: Int, distance: Distance, random: Random) {

  def this(f: Int, random: Random) = this(f, Angular, random)

  def this(f: Int, metric: Distance) = this(f, metric, RandRandom)

  def this(f: Int) = this(f, Angular, RandRandom)

  private val nodeSizeInBytes: Int = AngularNodeOperations.nodeSizeInBytes(dim)
  private val childrenCapacity: Int = AngularNodeOperations.childrenCapacity(dim)
  private var verbose0: Boolean = false
  private var nodes: NodeContainer = null
  private val roots = new ArrayBuffer[Int]()
  private var loaded: Boolean = false
  private var nItems: Int = 0
  private var nNodes: Int = 0

  reinitialize()

  def getBufferType: String = nodes.bufferType

  def getDim: Int = dim

  def addItem(item: Int, w: Array[Float]): Unit = {
    ensureSize(item + 1)
    val n = getNode(item)

    n.setChildren(0, 0)
    n.setChildren(1, 0)
    n.setNDescendants(1)
    n.setV(w)

    if (item >= nItems)
      nItems = item + 1
  }

  def build(q: Int): Unit = {
    require(!loaded, "You can't build a loaded index")

    nNodes = nItems
    while ((q != -1 || nNodes < nItems * 2) && (q == -1 || roots.length < q)) {
      if (verbose0) showUpdate("pass %d...\n", roots.length)
      val indices = new ArrayBuffer(nItems) ++= (0 until nItems)
      val x = makeTree(indices)
      roots += x
    }

    // Also, copy the roots into the last segment of the array
    // This way we can load them faster without reading the whole file
    ensureSize(nNodes + roots.length)
    roots.zipWithIndex.foreach { case (root, i) =>
      getNode(nNodes + i).copyFrom(getNode(root))
    }
    nNodes += roots.length
    nodes.flip()

    if (verbose0) showUpdate("has %d nodes\n", nNodes)
  }

  def save(filename: String, reload: Boolean = true): Boolean = {
    nodes match {
      case heapNodes: HeapNodeContainer[_] =>
        heapNodes.prepareToWrite()
        val fs = new FileOutputStream(filename).getChannel
        fs.write(heapNodes.underlying)
        fs.close()
      case _ =>
    }
    if (reload) {
      unload()
      load(filename)
    } else {
      true
    }
  }

  def unload(): Unit = {
    nodes match {
      case mappedNodes: MappedNodeContainer[_] =>
        mappedNodes.close()
      case _ =>
    }
    reinitialize()
    if (verbose0) showUpdate("unloaded\n")
  }

  def load(filename: String, useHeap: Boolean = false): Boolean = {
    val nodesOnFile = new MappedNodeContainer[AngularNodeOperations](dim, filename)
    nodes = nodesOnFile
    nNodes = nodes.getSize / nodeSizeInBytes
    var m = -1
    var i = nNodes - 1
    while (i >= 0) {
      val k = getNode(i).getNDescendants
      if (m == -1 || k == m) {
        roots += i
        m = k
      } else {
        i = 0 // break
      }
      i -= 1
    }

    if (roots.length > 1 && getNode(roots.head).getChildren(0) == getNode(roots.last).getChildren(0)) {
      roots -= roots.last // pop_back
    }
    loaded = true
    nItems = m

    if (useHeap) {
      val nodesOnHeap = new HeapNodeContainer[AngularNodeOperations](dim, nNodes)
      nodesOnFile.underlying.rewind()
      nodesOnHeap.underlying.put(nodesOnFile.underlying)
      nodes = nodesOnHeap
      nodesOnFile.close()
    }

    if (verbose0) showUpdate("found %d roots with degree %d\n", roots.length, m)
    true
  }

  def verbose(v: Boolean): Unit = this.verbose0 = v

  def getNItems: Int = nItems

  def getItem(item: Int): Array[Float] = getNode(item).getVector(new Array[Float](dim))

  def getNnsByItem(item: Int, n: Int): Array[(Int, Float)] = getNnsByItem(item, n, -1)

  def getNnsByItem(item: Int, n: Int, k: Int): Array[(Int, Float)] = {
    val v = getNode(item).getVector(new Array[Float](dim))
    getAllNns(v, n, k)
  }

  def getNnsByVector(w: Array[Float], n: Int): Array[(Int, Float)] = getNnsByVector(w, n, -1)

  def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = getAllNns(w, n, k)

  private def getAllNns(v: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    val vectorBuffer = new Array[Float](dim)
    val searchK = if (k == -1) n * roots.length else k

    val q = new mutable.PriorityQueue[(Float, Int)] ++= roots.map(Float.PositiveInfinity -> _)

    val nns = new ArrayBuffer[Int](searchK)
    val childrenBuffer = new Array[Int](childrenCapacity)
    while (nns.length < searchK && q.nonEmpty) {
      val top = q.dequeue()
      val d = top._1
      val i = top._2
      val nd = getNode(i)
      val nDescendants = nd.getNDescendants
      if (nDescendants == 1 && i < nItems) {
        nns += i
      } else if (nDescendants <= childrenCapacity) {
        nd.getAllChildren(childrenBuffer)
        var jj = 0
        while (jj < nDescendants) {
          nns += childrenBuffer(jj)
          jj += 1
        }
      } else {
        val margin = distance.margin(nd, v, vectorBuffer)
        q += math.min(d, +margin) -> nd.getChildren(1)
        q += math.min(d, -margin) -> nd.getChildren(0)
      }
    }

    // Get distances for all items
    // To avoid calculating distance multiple times for any items, sort by id
    val sortedNns = nns.toArray
    java.util.Arrays.sort(sortedNns)
    val nnsDist = new ArrayBuffer[(Float, Int)](sortedNns.length)
    var last = -1
    var i = 0
    while (i < sortedNns.length) {
      val j = sortedNns(i)
      if (j != last) {
        last = j
        nnsDist += distance.distance(v, getNode(j).getVector(vectorBuffer)) -> j
      }
      i += 1
    }

    val m = nnsDist.length
    val p = math.min(n, m)

    val partialSorted = new TopK[(Float, Int)](p, reversed = true)
    nnsDist.foreach(partialSorted += _)

    partialSorted
      .map { case (dist, item) =>
        (item, distance.normalizeDistance(dist))
      }
      .toArray
  }

  def getDistance(i: Int, j: Int): Float = {
    distance.distance(nodes(i).getVector(new Array[Float](dim)), nodes(j).getVector(new Array[Float](dim)))
  }

  private def getNode(item: Int): Node = nodes(item)

  private def getNodeOrNull(item: Int): Node = {
    val n = nodes(item)
    if (n.getNDescendants == 0) null else n
  }

  private def reinitialize(): Unit = {
    nodes = null
    loaded = false
    nItems = 0
    nNodes = 0
    roots.clear()
  }

  private def ensureSize(n: Int): Unit = {
    if (nodes == null)
      nodes = new HeapNodeContainer[AngularNodeOperations](dim, 0)
    nodes.ensureSize(n, verbose0)
  }

  private def makeTree(indices: ArrayBuffer[Int]): Int = {
    if (indices.length == 1)
      return indices(0)

    if (indices.length <= childrenCapacity) {
      ensureSize(nNodes + 1)
      val item = nNodes
      nNodes += 1
      val m = getNode(item)
      m.setNDescendants(indices.length)
      m.setAllChildren(indices.toArray)
      return item
    }

    val children = new ArrayBuffer[Node]()
    var i = 0
    while (i < indices.length) {
      val j = indices(i)
      val n = getNodeOrNull(j)
      if (n != null)
        children += n
      i += 1
    }

    val childrenIndices = Array.fill(2) {
      new ArrayBuffer[Int]
    }

    val m = nodes.newNode
    distance.createSplit(children, dim, random, m)

    val vectorBuffer = new Array[Float](dim)
    val sideBuffer = new Array[Float](dim)
    i = 0
    while (i < indices.length) {
      val j = indices(i)
      val n = getNodeOrNull(j)
      if (n != null) {
        val side = if (distance.side(m, n.getVector(vectorBuffer), random, sideBuffer)) 1 else 0
        childrenIndices(side) += j
      }
      i += 1
    }

    // If we didn't find a hyperplane, just randomize sides as a last option
    while (childrenIndices(0).isEmpty || childrenIndices(1).isEmpty) {
      if (verbose0 && indices.length > 100000)
        showUpdate("Failed splitting %d items\n", indices.length)

      childrenIndices(0).clear()
      childrenIndices(1).clear()

      // Set the vector to 0.0
      m.setValue(0f)

      var i = 0
      while (i < indices.length) {
        val j = indices(i)
        // Just randomize...
        childrenIndices(if (random.flip()) 1 else 0) += j
        i += 1
      }
    }

    val flip = if (childrenIndices(0).length > childrenIndices(1).length) 1 else 0

    m.setNDescendants(indices.length)
    var side = 0
    while (side < 2) {
      m.setChildren(side ^ flip, makeTree(childrenIndices(side ^ flip)))
      side += 1
    }
    ensureSize(nNodes + 1)
    val item = nNodes
    nNodes += 1
    getNode(item).copyFrom(m)
    item
  }
}

// code from https://github.com/scalanlp/breeze/blob/42c2e2522cf09259a34879e1c3b13b81176e410f/math/src/main/scala/breeze/util/TopK.scala
class TopK[T](k : Int, reversed: Boolean = false)(implicit ord : Ordering[T]) extends Iterable[T] {
  import scala.collection.JavaConversions._

  val _ord = if (reversed) ord.reverse else ord

  private val keys = new util.TreeSet[T](_ord)

  def +=(e : T) = {
    if (keys.size < k) {
      keys.add(e)
    } else if (keys.size > 0 && _ord.lt(keys.first, e) && !keys.contains(e)) {
      keys.remove(keys.first)
      keys.add(e)
    }
  }

  override def iterator : Iterator[T] =
    keys.descendingIterator

  override def size = keys.size

}
