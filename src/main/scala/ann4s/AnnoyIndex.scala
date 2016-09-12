package ann4s

import java.io.FileOutputStream

import ann4s.Functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class AnnoyIndex(dim: Int, metric: Metric, random: Random) {

  def this(f: Int, random: Random) = this(f, Angular, random)

  def this(f: Int, metric: Metric) = this(f, metric, RandRandom)

  def this(f: Int) = this(f, Angular, RandRandom)

  private val childrenCapacity: Int = metric.childrenCapacity(dim)

  private var verbose0: Boolean = false

  private var nodes: NodeStorage = null

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
      case heapNodes: HeapNodeStorage =>
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
      case mappedNodes: MappedNodeStorage =>
        mappedNodes.close()
      case _ =>
    }
    reinitialize()
    if (verbose0) showUpdate("unloaded\n")
  }

  def load(filename: String, useHeap: Boolean = false): Boolean = {
    val nodesOnFile = new MappedNodeStorage(dim, filename, metric)
    nodes = nodesOnFile
    nNodes = nodes.numNodes
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
      val nodesOnHeap = new HeapNodeStorage(dim, nNodes, metric)
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
        val margin = metric.margin(nd, v, vectorBuffer)
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
        nnsDist += metric.distance(v, getNode(j).getVector(vectorBuffer)) -> j
      }
      i += 1
    }

    val m = nnsDist.length
    val p = math.min(n, m)

    val partialSorted = new TopK[(Float, Int)](p, reversed = true)
    nnsDist.foreach(partialSorted += _)

    partialSorted
      .map { case (dist, item) =>
        (item, metric.normalizeDistance(dist))
      }
      .toArray
  }

  def getDistance(i: Int, j: Int): Float = {
    metric.distance(nodes(i).getVector(new Array[Float](dim)), nodes(j).getVector(new Array[Float](dim)))
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
      nodes = new HeapNodeStorage(dim, 0, metric)
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
    metric.createSplit(children, dim, random, m)

    val vectorBuffer = new Array[Float](dim)
    val sideBuffer = new Array[Float](dim)
    i = 0
    while (i < indices.length) {
      val j = indices(i)
      val n = getNodeOrNull(j)
      if (n != null) {
        val side = if (metric.side(m, n.getVector(vectorBuffer), random, sideBuffer)) 1 else 0
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

