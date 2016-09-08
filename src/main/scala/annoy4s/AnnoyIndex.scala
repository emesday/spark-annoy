package annoy4s

import java.io.FileOutputStream
import java.nio.{ByteOrder, ByteBuffer}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random => RND}

trait Node {
  def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int) : Int
  def getChildren(underlying: ByteBuffer, offsetInByte: Int,  i: Int): Int
  def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int]
  def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float]

  def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit
  def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit
  def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit
  def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit

  def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit
}

case class N(dim: Int, nodeSizeInBytes: Int, underlying: ByteBuffer, offsetInBytes: Int, ops: Node) {

  def getNDescendants: Int =
    ops.getNDescendants(underlying, offsetInBytes)

  def getChildren(i: Int): Int =
    ops.getChildren(underlying, offsetInBytes, i)

  def getAllChildren(dst: Array[Int]): Array[Int] =
    ops.getAllChildren(underlying, offsetInBytes, dst)

  def getV(dst: Array[Float]): Array[Float] =
    ops.getV(underlying, offsetInBytes, dst)

  def setNDescendants(nDescendants: Int) =
    ops.setNDescendants(underlying, offsetInBytes, nDescendants)

  def setChildren(i: Int, v: Int): Unit =
    ops.setChildren(underlying, offsetInBytes, i, v)

  def setAllChildren(indices: Array[Int]): Unit =
    ops.setAllChildren(underlying, offsetInBytes, indices)

  def setV(v: Array[Float]): Unit =
    ops.setV(underlying, offsetInBytes, v)

  def copyFrom(other: N): Unit = {
    require(ops == other.ops)
    ops.copy(other.underlying, other.offsetInBytes, underlying, offsetInBytes, nodeSizeInBytes)
  }
}

class Nodes[T <: Node](dim: Int, _size: Int) {

  val reallocation_factor = 1.3

  val (nodeSizeInBytes, childrenCapacity, ops) =  {
//    case cls if classOf[AngularNode].isAssignableFrom(cls) =>
      (AngularNode.nodeSizeInBytes(dim), AngularNode.childrenCapacity(dim), AngularNode)
  }

  var size = _size
  var bytes = new Array[Byte](nodeSizeInBytes * size)
  var underlying = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)

  def ensureSize(n: Int, _verbose: Boolean): Int = {
    if (n > size) {
      val newsize = math.max(n, (size + 1) * reallocation_factor).toInt
      if (_verbose) showUpdate("Reallocating to %d nodes\n", newsize)
      val newBytes: Array[Byte] = new Array(nodeSizeInBytes * newsize)
      scala.compat.Platform.arraycopy(bytes, 0, newBytes, 0, bytes.length)

      bytes = newBytes
      underlying = ByteBuffer.wrap(newBytes).order(ByteOrder.LITTLE_ENDIAN)
      size = newsize
    }
    size
  }

  def apply(i: Int): N = N(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, ops)

  def alloc: N = N(dim, nodeSizeInBytes, ByteBuffer.wrap(new Array[Byte](nodeSizeInBytes)), 0, ops)
}

trait AngularNode extends Node {
  @inline def nodeSizeInBytes(f: Int): Int = 12 + f * 4
  @inline def childrenCapacity(f: Int): Int = 2 + f

  // n_descendants: Int = 4
  // n_children[0]: Int = 4
  // n_children[1]: Int = 4
  // v: Array[Float] = f * 4

  override def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int): Int = {
    underlying.position(offsetInBytes)
    underlying.getInt(0)
  }

  override def getChildren(underlying: ByteBuffer, offsetInByte: Int, i: Int): Int = {
    underlying.position(offsetInByte)
    underlying.getInt(4 * (i + 1))
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

  override def setChildren(underlying: ByteBuffer, offsetIntBytes: Int, i: Int, v: Int): Unit = {
    underlying.position(offsetIntBytes + 4 * (i + 1))
    underlying.putInt(v)
  }

  override def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit = {
    underlying.position(offsetInBytes + 4)
    underlying.asIntBuffer().put(indices)
  }

  override def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit = {
    underlying.position(offsetInBytes + 12)
    underlying.asFloatBuffer().put(v)
  }

  override def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit = {
    val dup = src.duplicate()
    dup.position(srcOffsetInBytes)
    dup.limit(srcOffsetInBytes + nodeSizeInBytes)

    dst.position(dstOffsetInBytes)
    dst.put(dup)
  }

}

object AngularNode extends AngularNode

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
  override def flip(): Boolean = RND.nextBoolean()
  override def index(n: Int): Int = RND.nextInt(n)
}

object Functions {

  def getNorm(v: Array[Float]): Float = blas.snrm2(v.length, v, 1)

  def normalize(v: Array[Float]): Unit = blas.sscal(v.length, One / getNorm(v), v, 1)

  def twoMeans(nodes: ArrayBuffer[N], cosine: Boolean, iv: Array[Float], jv: Array[Float], metric: Distance, rand: Random): Unit = {
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
    val vj = new Array[Float](f)
    while (l < iterationSteps) {
      val k = rand.index(count)
      val di = ic * metric.distance(iv, nodes(k).getV(vi))
      val dj = jc * metric.distance(jv, nodes(k).getV(vj))
      val norm = if (cosine) getNorm(nodes(k).getV(vi)) else One
      if (di < dj) {
        z = 0
        while (z < f) {
          iv(z) = (iv(z) * ic + nodes(k).getV(vi)(z) / norm) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        z = 0
        while (z < f) {
          jv(z) = (jv(z) * jc + nodes(k).getV(vi)(z) / norm) / (jc + 1)
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
  def createSplit(nodes: ArrayBuffer[N], f: Int, rand: Random, n: N): Unit
  def side(n: N, y: Array[Float], random: Random, buffer: Array[Float]): Boolean
  def margin(n: N, y: Array[Float], buffer: Array[Float]): Float
  def normalizeDistance(distance: Float): Float
}

object Angular extends Distance {

  override val name = "angular"

  override def distance(x: Array[Float], y: Array[Float]): Float = {
    require(x.length == y.length)
    val pp = blas.sdot(x.length, x, 1, x, 1)
    val qq = blas.sdot(y.length, y, 1, y, 1)
    val pq = blas.sdot(x.length, x, 1, y, 1)
    val ppqq: Double = pp * qq
    if (ppqq > 0) (2.0 - 2.0 * pq / Math.sqrt(ppqq)).toFloat else 2.0f
  }

  override def margin(n: N, y: Array[Float], buffer: Array[Float]): Float = {
    blas.sdot(y.length, n.getV(buffer), 1, y, 1)
  }

  override def side(n: N, y: Array[Float], random: Random, buffer: Array[Float]): Boolean = {
    val dot = margin(n, y, buffer)
    if (dot != Zero) {
      dot > 0
    } else {
      random.flip()
    }
  }

  override def createSplit(nodes: ArrayBuffer[N], f: Int, rand: Random, n: N): Unit = {
    val buffer = new Array[Float](f)
    val bestIv = new Array[Float](f)
    val bestJv = new Array[Float](f)
    Functions.twoMeans(nodes, true, bestIv, bestJv, this, rand)
    var z = 0
    n.getV(buffer)
    while (z < f) {
      buffer(z) = bestIv(z) - bestJv(z)
      z += 1
    }
    Functions.normalize(buffer)
  }

  override def normalizeDistance(distance: Float): Float = {
    math.sqrt(math.max(distance, Zero)).toFloat
  }
}

class AnnoyIndex(f: Int, distance: Distance, _random: Random) extends AnnoyIndexInterface {

  def this(f: Int, metric: Distance) = this(f, metric, RandRandom)

  def this(f: Int) = this(f, Angular, RandRandom)

  val _s: Int = AngularNode.nodeSizeInBytes(f)
  val _K: Int = AngularNode.childrenCapacity(f)
  var _verbose: Boolean = false
  var _fd = 0
  var _nodes: Nodes[AngularNode] = null
  val _roots = new ArrayBuffer[Int]()
  var _loaded: Boolean = false
  var _n_items: Int = 0
  var _n_nodes: Int = 0

  reinitialize()

  def get_f(): Int = f

  def _get(item: Int): N = _nodes(item)

  def _getOrNull(item: Int): N = ??? //_nodes(item)

  override def addItem(item: Int, w: Array[Float]): Unit = {
    _alloc_size(item + 1)
    val n = _get(item)

    n.setChildren(0, 0)
    n.setChildren(1, 0)
    n.setNDescendants(1)
    n.setV(w)

    if (item >= _n_items)
      _n_items = item + 1
  }

  override def build(q: Int): Unit = {
    require(!_loaded, "You can't build a loaded index")

    _n_nodes = _n_items
    while ((q != -1 || _n_nodes < _n_items * 2) && (q == -1 || _roots.length < q)) {
      if (_verbose) showUpdate("pass %d...\n", _roots.length)
      val indices = new ArrayBuffer(_n_items) ++= (0 until _n_items)
      _roots += _make_tree(indices)
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

  override def save(filename: String): Boolean = {
    _nodes.underlying.rewind()
    val fs = new FileOutputStream(filename).getChannel()
    fs.write(_nodes.underlying)
    fs.close()
    true
  }

  def reinitialize(): Unit = {
    _fd = 0
    _nodes = new Nodes[AngularNode](f, 0)
    _loaded = false
    _n_items = 0
    _n_nodes = 0
    _roots.clear()
  }

  override def unload(): Unit = {}

  override def load(filename: String): Boolean = false

  override def verbose(v: Boolean): Unit = this._verbose = v

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

    val children = new ArrayBuffer[N]()
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

    /*
    // If we didn't find a hyperplane, just randomize sides as a last option
    while (children_indices[0].size() == 0 || children_indices[1].size() == 0) {
      if (_verbose && indices.size() > 100000)
        showUpdate("Failed splitting %lu items\n", indices.size());

      children_indices[0].clear();
      children_indices[1].clear();

      // Set the vector to 0.0
      for (int z = 0; z < _f; z++)
      m->v[z] = 0.0;

      for (size_t i = 0; i < indices.size(); i++) {
        S j = indices[i];
        // Just randomize...
        children_indices[_random.flip()].push_back(j);
      }
    }
    */
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

  override def getNItems: Int = ???

  override def getItem(item: Int): Array[Float] = ???


  override def getNnsByItem(item: Int, n: Int, k: Int): Array[(Int, Float)] = {
    val v = new Array[Float](f)
    _get(item).getV(v)
    _get_all_nns(v, n, k)
  }

  override def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    _get_all_nns(w, n, k)
  }

  def _get_all_nns(v: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {

    val v0 = new Array[Float](f)

    // implicit val ord = Ordering.by[(Float, Int), Float](x => x._1)
    val q = new mutable.PriorityQueue[(Float, Int)]
    val search_k = if (k == -1) n * _roots.length else k

    _roots.foreach { root =>
      q += Float.PositiveInfinity -> root
    }

    var nns = new ArrayBuffer[Int]()
    val buffer = new Array[Int](_K)
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
        nns ++= nd.getAllChildren(buffer).take(nDescendants)
//        var jj = 0
//        while (jj < nDescendants) {
//          nns += buffer(jj)
//          jj += 1
//        }
      } else {
        val margin = distance.margin(nd, v, v0)
        q += math.min(d, +margin) -> nd.getChildren(1)
        q += math.min(d, -margin) -> nd.getChildren(0)
      }
    }

    // Get distances for all items
    // To avoid calculating distance multiple times for any items, sort by id
    nns = nns.sorted
    val nns_dist = new ArrayBuffer[(Float, Int)]()
    var last = -1
    var i = 0
    while (i < nns.length) {
      val j = nns(i)
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

  override def getDistance(i: Int, j: Int): Float = ???
}
