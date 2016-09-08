package annoy4s

import java.nio.{ByteBuffer, FloatBuffer}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random => RND}

trait Node {

  def nDescendants: Int
  def children(n: Int): Int
  def getAllChildren(to: Array[Int]): Array[Int]
  def v: Array[Float]
  def vTo(to: Array[Float]): Array[Float]
  def vBuffer: FloatBuffer

  def setNDescendants(n_descendants: Int): Unit
  def setChildren(n: Int)(v: Int): Unit
  def setAllChildren(indices: Array[Int]): Unit
  def setV(v: Array[Float]): Unit

  def copyFrom(other: Node): Unit
  def getBytes: Array[Byte]
}

object AngularNode {
  @inline def s(f: Int): Int = 12 + f * 4
  @inline def k(f: Int): Int = 2 + f
}

class AngularNode(f: Int) extends Node {
  // n_descendants: Int = 4
  // n_children[0]: Int = 4
  // n_children[1]: Int = 4
  // v: Array[Float] = f * 4
  private val underlying = new Array[Byte](AngularNode.s(f))

  override def nDescendants: Int = ByteBuffer.wrap(underlying, 0, 4).getInt
  override def children(n: Int): Int = ByteBuffer.wrap(underlying, 4 * (n + 1), 4).getInt
  override def getAllChildren(to: Array[Int]): Array[Int] = {
    ByteBuffer.wrap(underlying, 4, nDescendants * 4).asIntBuffer().get(to, 0, nDescendants)
    to
  }

  override def v: Array[Float] = {
    val a = new Array[Float](f)
    vBuffer.get(a)
    a
  }
  override def vTo(to: Array[Float]): Array[Float] = {
    vBuffer.get(to)
    to
  }

  override def vBuffer: FloatBuffer = ByteBuffer.wrap(underlying, 12, f * 4).asFloatBuffer()

  override def setNDescendants(n_descendants: Int) =
    ByteBuffer.wrap(underlying, 0, 4).putInt(n_descendants)

  override def setChildren(n: Int)(v: Int) =
    ByteBuffer.wrap(underlying, 4 * n, 4).putInt(v)

  override def setAllChildren(indices: Array[Int]): Unit = {
    ByteBuffer.wrap(underlying, 4, indices.length * 4).asIntBuffer().put(indices, 0, indices.length)
  }

  override def setV(v: Array[Float]) = {
    ByteBuffer.wrap(underlying, 12, f * 4).asFloatBuffer().put(v)
  }

  override def getBytes: Array[Byte] = underlying

  override def copyFrom(other: Node): Unit = {
    other match {
      case o: AngularNode =>
        scala.compat.Platform.arraycopy(o.underlying, 0, underlying, 0, o.underlying.length)
      case _ =>
        throw new IllegalArgumentException
    }
  }

}

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

  def twoMeans(nodes: ArrayBuffer[Node], cosine: Boolean, iv: Array[Float], jv: Array[Float], metric: Distance, rand: Random): Unit = {
    val iterationSteps = 200
    val count = nodes.length
    val f = iv.length

    val i = rand.index(count)
    var j = rand.index(count - 1)
    j += (if (j >= i) 1 else 0)
    System.arraycopy(nodes(i).v, 0, iv, 0, f)
    System.arraycopy(nodes(j).v, 0, jv, 0, f)
    if (cosine) {
      normalize(iv)
      normalize(jv)
    }

    var ic = 1
    var jc = 1
    var l = 0
    var z = 0
    while (l < iterationSteps) {
      val k = rand.index(count)
      val di = ic * metric.distance(iv, nodes(k).v)
      val dj = jc * metric.distance(jv, nodes(k).v)
      val norm = if (cosine) getNorm(nodes(k).v) else One
      if (di < dj) {
        z = 0
        while (z < f) {
          iv(z) = (iv(z) * ic + nodes(k).v(z) / norm) / (ic + 1)
          z += 1
        }
        ic += 1
      } else if (dj < di) {
        z = 0
        while (z < f) {
          jv(z) = (jv(z) * jc + nodes(k).v(z) / norm) / (jc + 1)
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
  def side(n: Node, y: Array[Float], random: Random): Boolean
  def margin(n: Node, y: Array[Float]): Float
  def newNode(f: Int): Node
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

  override def margin(n: Node, y: Array[Float]): Float = {
    blas.sdot(y.length, n.v, 1, y, 1)
  }

  override def side(n: Node, y: Array[Float], random: Random): Boolean = {
    val dot = margin(n, y)
    if (dot != Zero) {
      dot > 0
    } else {
      random.flip()
    }
  }

  override def createSplit(nodes: ArrayBuffer[Node], f: Int, rand: Random, n: Node): Unit = {
    val bestIv = new Array[Float](f)
    val bestJv = new Array[Float](f)
    Functions.twoMeans(nodes, true, bestIv, bestJv, this, rand)
    var z = 0
    while (z < f) {
      n.v(z) = bestIv(z) - bestJv(z)
      z += 1
    }
    Functions.normalize(n.v)
  }

  override def normalizeDistance(distance: Float): Float = {
    math.sqrt(math.max(distance, Zero)).toFloat
  }

  override def newNode(f: Int): Node = new AngularNode(f)

}

class AnnoyIndex(f: Int, distance: Distance, _random: Random, initialSize: Int) extends AnnoyIndexInterface {

  def this(f: Int, metric: Distance) = this(f, metric, RandRandom, 16)

  def this(f: Int) = this(f, Angular, RandRandom, 16)

  val _s: Int = AngularNode.s(f)
  val _K: Int = AngularNode.k(f)
  var _verbose: Boolean = false
  var _fd = 0
  var _nodes: Array[Node] = null
  var _nodes_size = 0
  val _roots = new ArrayBuffer[Int]()
  var _loaded: Boolean = false
  var _n_items: Int = 0
  var _n_nodes: Int = 0

  reinitialize()

  def get_f(): Int = f

  def _get(item: Int): Node = {
    var n = _nodes(item)
    if (n == null) {
      n = distance.newNode(f)
      _nodes(item) = n
    }
    n
  }

  def _getOrNull(item: Int): Node = _nodes(item)

  override def addItem(item: Int, w: Array[Float]): Unit = {
    _alloc_size(item + 1)
    val n = _get(item) //distance.newNode()

    n.setChildren(0)(0)
    n.setChildren(1)(0)
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

  override def save(filename: String): Boolean = false

  def reinitialize(): Unit = {
    _fd = 0
    _nodes = new Array[Node](initialSize)
    _loaded = false
    _n_items = 0
    _n_nodes = 0
    _roots.clear()
  }

  override def unload(): Unit = {}

  override def load(filename: String): Boolean = false

  override def verbose(v: Boolean): Unit = this._verbose = v

  val reallocation_factor = 1.3

  private def _alloc_size(n: Int): Unit = {
    val array = _nodes
    if (n > _nodes_size) {
      val newsize = math.max(n, (_nodes_size + 1) * reallocation_factor).toInt
      if (_verbose) showUpdate("Reallocating to %d nodes\n", newsize)

      val newar: Array[Node] = new Array(newsize)
      scala.compat.Platform.arraycopy(array, 0, newar, 0, _nodes_size)
      _nodes = newar
      _nodes_size = newsize
    }
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

    val v = new Array[Float](f)
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

    val m = distance.newNode(f)
    distance.createSplit(children, f, _random, m)

    i = 0
    while (i < indices.length) {
      val j = indices(i)
      val n = _getOrNull(j)
      if (n != null) {
        val side = if (distance.side(m, n.vTo(v), _random)) 1 else 0
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
      m.setChildren(side ^ flip)(_make_tree(childrenIndices(side ^ flip)))
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
    val m = _get(item)
    _get_all_nns(m.v, n, k)
  }

  override def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    _get_all_nns(w, n, k)
  }

  def _get_all_nns(v: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
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
      val nd = _getOrNull(i)
      q.dequeue()
      if (nd.nDescendants == 1 && i < _n_items) {
        nns += i
      } else if (nd.nDescendants <= _K) {
        nns ++= nd.getAllChildren(buffer).take(nd.nDescendants)
      } else {
        val margin = distance.margin(nd, v)
        q += math.min(d, +margin) -> nd.children(1)
        q += math.min(d, -margin) -> nd.children(0)
      }
    }

    // Get distances for all items
    // To avoid calculating distance multiple times for any items, sort by id
    val vBuffer = new Array[Float](f)
    nns = nns.sorted
    val nns_dist = new ArrayBuffer[(Float, Int)]()
    var last = -1
    var i = 0
    while (i < nns.length) {
      val j = nns(i)
      if (j != last) {
        last = j
        nns_dist += distance.distance(v, _get(j).vTo(vBuffer)) -> j
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
