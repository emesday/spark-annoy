package annoy4s

import java.io.FileOutputStream
import java.nio.{ByteBuffer, ByteOrder}

import annoy4s.Functions._

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.util.{Random => RND}

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

case class Node(dim: Int, nodeSizeInBytes: Int, underlying: ByteBuffer, offsetInBytes: Int, ops: AngularNodeOperations) {

  def getNDescendants: Int =
    ops.getNDescendants(underlying, offsetInBytes)

  def getChildren(i: Int): Int =
    ops.getChildren(underlying, offsetInBytes, i)

  def getAllChildren(dst: Array[Int]): Array[Int] =
    ops.getAllChildren(underlying, offsetInBytes, dst)

  def getV(dst: Array[Float]): Array[Float] =
    ops.getV(underlying, offsetInBytes, dst)

  def setValue(v: Float): Unit =
    ops.setValue(underlying, offsetInBytes, v, dim)

  def setNDescendants(nDescendants: Int) =
    ops.setNDescendants(underlying, offsetInBytes, nDescendants)

  def setChildren(i: Int, v: Int): Unit =
    ops.setChildren(underlying, offsetInBytes, i, v)

  def setAllChildren(indices: Array[Int]): Unit =
    ops.setAllChildren(underlying, offsetInBytes, indices)

  def setV(v: Array[Float]): Unit =
    ops.setV(underlying, offsetInBytes, v)

  def copyFrom(other: Node): Unit = {
//    val in = other.getV(new Array[Float](dim))
//    println(s"copyFrom: in ${in.mkString(",")}")
    ops.copy(other.underlying, other.offsetInBytes, underlying, offsetInBytes, nodeSizeInBytes)
//    val out = this.getV(new Array[Float](dim))
//    println(s"copyFrom: out ${out.mkString(",")}")
//    require(out.zip(in).forall(x => x._1 == x._2))
  }

}

class Nodes[T <: NodeOperations](dim: Int, _size: Int) {

  val reallocation_factor = 1.3

  val (nodeSizeInBytes, childrenCapacity, ops) =  {
    //    case cls if classOf[AngularNode].isAssignableFrom(cls) =>
    (AngularNodeOperations.nodeSizeInBytes(dim), AngularNodeOperations.childrenCapacity(dim), AngularNodeOperations)
  }

  var size = _size
  var underlying = ByteBuffer.allocate(nodeSizeInBytes * size).order(ByteOrder.LITTLE_ENDIAN)

  def ensureSize(n: Int, _verbose: Boolean): Int = {
    if (n > size) {
      val newsize = math.max(n, (size + 1) * reallocation_factor).toInt
      if (_verbose) showUpdate("Reallocating to %d nodes\n", newsize)
      val newBuffer: ByteBuffer = ByteBuffer.allocateDirect(nodeSizeInBytes * newsize).order(ByteOrder.LITTLE_ENDIAN)

      underlying.rewind()
      newBuffer.put(underlying)
      underlying = newBuffer
      size = newsize
    }
    size
  }

  def apply(i: Int): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, ops)

  def alloc: Node = Node(dim, nodeSizeInBytes, ByteBuffer.allocate(nodeSizeInBytes).order(ByteOrder.LITTLE_ENDIAN), 0, ops)
}

/**
  * n_descendants: Int = 4
  * n_children[0]: Int = 4
  * n_children[1]: Int = 4
  * v: Array[Float] = f * 4
  */
trait AngularNodeOperations extends NodeOperations {

  def nodeSizeInBytes(f: Int): Int = 12 + f * 4
  def childrenCapacity(f: Int): Int = 2 + f

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
    ???
    underlying.position(offsetInBytes + 12)
    var i = 0
    while (i < dim) {
      underlying.asFloatBuffer().put(i, v)
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
  val rnd = new RND(0)
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
    var sq_norm: Double = 0
    var z = 0
    while (z < x.length) {
      sq_norm += x(z) * x(z)
      z += 1
    }
    math.sqrt(sq_norm).toFloat
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

  def showUpdate(text: String, xs: Any*): Unit = Console.err.print(text.format(xs: _*))

  def getNorm(v: Array[Float]): Float = blas.nrm2(v)

  def normalize(v: Array[Float]): Unit = blas.scal(One / getNorm(v), v)

  def twoMeans(nodes: ArrayBuffer[Node], cosine: Boolean, iv: Array[Float], jv: Array[Float], metric: Distance, rand: Random): Unit = {
    val iterationSteps = 200
    val count = nodes.length
    val f = iv.length

    val i = rand.index(count)
    var j = rand.index(count - 1)
    j += (if (j >= i) 1 else 0)
    nodes(i).getV(iv)
    nodes(j).getV(jv)

    if (cosine) {
      normalize(iv)
      normalize(jv)
    }

    var ic = 1
    var jc = 1
    var l = 0
    var z = 0
    val vi = new Array[Float](f)
    while (l < iterationSteps) {
      val k = rand.index(count)
      val zz = nodes(k).getV(vi)
      val di = ic * metric.distance(iv, zz)
      val dj = jc * metric.distance(jv, zz)
      val norm = if (cosine) getNorm(zz) else One
      if (di < dj) {
        z = 0
        while (z < f) {
          iv(z) = (iv(z) * ic + zz(z) / norm) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        z = 0
        while (z < f) {
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

  override def margin(n: Node, y: Array[Float], buffer: Array[Float]): Float = blas.dot(n.getV(buffer), y)

  override def side(n: Node, y: Array[Float], random: Random, buffer: Array[Float]): Boolean = {
    val dot = margin(n, y, buffer)
    if (dot != Zero) {
      dot > 0
    } else {
      random.flip()
    }
  }

  override def createSplit(nodes: ArrayBuffer[Node], f: Int, rand: Random, n: Node): Unit = {
    val buffer = new Array[Float](f)
    val bestIv = new Array[Float](f)
    val bestJv = new Array[Float](f)
    twoMeans(nodes, true, bestIv, bestJv, this, rand)
    var z = 0
    n.getV(buffer)
    while (z < f) {
      buffer(z) = bestIv(z) - bestJv(z)
      z += 1
    }
//    println(s"buffer: ${buffer.mkString(",")}")
    normalize(buffer)
    n.setV(buffer)
  }

  override def normalizeDistance(distance: Float): Float = {
    math.sqrt(math.max(distance, Zero)).toFloat
  }
}

class AnnoyIndex(f: Int, distance: Distance, _random: Random) {

  def this(f: Int, random: Random) = this(f, Angular, random)

  def this(f: Int, metric: Distance) = this(f, metric, RandRandom)

  def this(f: Int) = this(f, Angular, RandRandom)

  val _s: Int = AngularNodeOperations.nodeSizeInBytes(f)
  val _K: Int = AngularNodeOperations.childrenCapacity(f)
  var _verbose: Boolean = false
  var _fd = 0
  var _nodes: Nodes[AngularNodeOperations] = null
  val _roots = new ArrayBuffer[Int]()
  var _loaded: Boolean = false
  var _n_items: Int = 0
  var _n_nodes: Int = 0

  reinitialize()

  def get_f(): Int = f

  def _get(item: Int): Node = _nodes(item)

  def _getOrNull(item: Int): Node = {
    val n = _nodes(item)
    if (n.getNDescendants == 0) null else n
  }

  def addItem(item: Int, w: Array[Float]): Unit = {
    _alloc_size(item + 1)
    val n = _get(item)

    n.setChildren(0, 0)
    n.setChildren(1, 0)
    n.setNDescendants(1)
    n.setV(w)

    if (item >= _n_items)
      _n_items = item + 1

  }

  def build(q: Int): Unit = {
    require(!_loaded, "You can't build a loaded index")

    _n_nodes = _n_items
    while ((q != -1 || _n_nodes < _n_items * 2) && (q == -1 || _roots.length < q)) {
      if (_verbose) showUpdate("pass %d...\n", _roots.length)
      val indices = new ArrayBuffer(_n_items) ++= (0 until _n_items)
      val x = _make_tree(indices)
      _roots += x
    }

    // Also, copy the roots into the last segment of the array
    // This way we can load them faster without reading the whole file
    _alloc_size(_n_nodes + _roots.length)
    _roots.zipWithIndex.foreach { case (root, i) =>
      _get(_n_nodes + i).copyFrom(_get(root))
    }
    _n_nodes += _roots.length

    if (_verbose) showUpdate("has %d nodes\n", _n_nodes)
  }

  def save(filename: String): Boolean = {
    _nodes.underlying.rewind()
    val fs = new FileOutputStream(filename).getChannel()
    fs.write(_nodes.underlying)
    fs.close()
    true
  }

  def reinitialize(): Unit = {
    _fd = 0
    _nodes = new Nodes[AngularNodeOperations](f, 0)
    _loaded = false
    _n_items = 0
    _n_nodes = 0
    _roots.clear()
  }

  def unload(): Unit = ???

  def load(filename: String): Boolean = ???

  def verbose(v: Boolean): Unit = this._verbose = v

  private def _alloc_size(n: Int): Unit = {
    _nodes.ensureSize(n, _verbose)
  }

  def _make_tree(indices: ArrayBuffer[Int]): Int = {
    if (indices.length == 1)
      return indices(0)

    if (indices.length <= _K) {
      _alloc_size(_n_nodes + 1)
      val item = _n_nodes
      _n_nodes += 1
      val m = _get(item)
      m.setNDescendants(indices.length)
      m.setAllChildren(indices.toArray)
      return item
    }

    val children = new ArrayBuffer[Node]()
    var i = 0
    while (i < indices.length) {
      val j = indices(i)
      val n = _getOrNull(j)
      if (n != null)
        children += n
      i += 1
    }

    val childrenIndices = Array.fill(2) {
      new ArrayBuffer[Int]
    }

    val m = _nodes.alloc

    distance.createSplit(children, f, _random, m)

    i = 0

    val v0 = new Array[Float](f)
    val v1 = new Array[Float](f)

    while (i < indices.length) {
      val j = indices(i)
      val n = _getOrNull(j)
      if (n != null) {
        val side = if (distance.side(m, n.getV(v0), _random, v1)) 1 else 0
        childrenIndices(side) += j
      }
      i += 1
    }

    // If we didn't find a hyperplane, just randomize sides as a last option
    while (childrenIndices(0).isEmpty || childrenIndices(1).isEmpty) {
      if (_verbose && indices.length > 100000)
        showUpdate("Failed splitting %lu items\n", indices.length)

      childrenIndices(0).clear()
      childrenIndices(1).clear()

      // Set the vector to 0.0
      m.setValue(0f)

      var i = 0
      while (i < indices.length) {
        val j = indices(i)
        // Just randomize...
        childrenIndices(if (_random.flip()) 1 else 0) += j
        i += 1
      }
    }

    val flip = if (childrenIndices(0).length > childrenIndices(1).length) 1 else 0

    m.setNDescendants(indices.length)
    var side = 0
    while (side < 2) {
      m.setChildren(side ^ flip, _make_tree(childrenIndices(side ^ flip)))
      side += 1
    }
    _alloc_size(_n_nodes + 1)
    val item = _n_nodes
    _n_nodes += 1
    _get(item).copyFrom(m)

    item
  }

  def getNItems: Int = _n_items

  def getItem(item: Int): Array[Float] = _get(item).getV(new Array[Float](f))

  val getNnsByItemV = new Array[Float](f)

  def getNnsByItem(item: Int, n: Int, k: Int): Array[(Int, Float)] = {
    _get(item).getV(getNnsByItemV)
    _get_all_nns(getNnsByItemV, n, k)
  }

  def getNnsByItem(item: Int, n: Int): Array[(Int, Float)] = getNnsByItem(item, n, -1)

  def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    _get_all_nns(w, n, k)
  }

  def getNnsByVector(w: Array[Float], n: Int): Array[(Int, Float)] = getNnsByVector(w, n, -1)

  val getAllNnsV = new Array[Float](f)
  val getAllNnsI = new Array[Int](_K)

  def _get_all_nns(v: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    val v0 = getAllNnsV
    // implicit val ord = Ordering.by[(Float, Int), Float](x => x._1)
    val q = new mutable.PriorityQueue[(Float, Int)]
    val search_k = if (k == -1) n * _roots.length else k

    _roots.foreach { root =>
      q += Float.PositiveInfinity -> root
    }

    var nns = new ListBuffer[Int]()
    val buffer = getAllNnsI
    while (nns.length < search_k && q.nonEmpty) {
      val top = q.head
      val d = top._1
      val i = top._2
      val nd = _get(i)
      q.dequeue()
      val nDescendants = nd.getNDescendants
      if (nDescendants == 1 && i < _n_items) {
        nns += i
      } else if (nDescendants <= _K) {
        nd.getAllChildren(buffer)
        var jj = 0
        while (jj < nDescendants) {
          nns += buffer(jj)
          jj += 1
        }
      } else {
        val margin = distance.margin(nd, v, v0)
        q += math.min(d, +margin) -> nd.getChildren(1)
        q += math.min(d, -margin) -> nd.getChildren(0)
      }
    }

    // Get distances for all items
    // To avoid calculating distance multiple times for any items, sort by id
    val sortedNns = nns.sortWith(_ > _)
    val nns_dist = new ArrayBuffer[(Float, Int)]()
    var last = -1
    var i = 0
    while (i < sortedNns.length) {
      val j = sortedNns(i)
      if (j != last) {
        last = j
        nns_dist += distance.distance(v, _get(j).getV(v0)) -> j
      }
      i += 1
    }

    val m = nns_dist.length
    val p = math.min(n, m)

    nns_dist.sortBy(_._1).take(p)
      .map { case (dist, item) =>
        (item, distance.normalizeDistance(dist))
      }
      .toArray
  }

  val x0: Array[Float] = new Array[Float](f)
  val x1: Array[Float] = new Array[Float](f)

  def getDistance(i: Int, j: Int): Float = distance.distance(_nodes(i).getV(x0), _nodes(j).getV(x1))
}

