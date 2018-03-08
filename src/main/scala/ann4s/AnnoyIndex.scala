package ann4s

import java.io._
import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

@deprecated("take a look at LocalBuilds in test")
class AnnoyIndex(f: Int, distance: Distance, random: Random) {

  def this(f: Int, distance: Distance, seed: Long) = this(f, distance, new Random(seed))

  def this(f: Int, distance: Distance) = this(f, distance, new Random)

  private val items = new ArrayBuffer[IdVectorWithNorm]()

  private var underlying: Index = _

  def addItem(i: Int, v: Array[Float]): Unit = {
    items += IdVectorWithNorm(i, SVector(v.clone()))
  }

  def build(numTrees: Int): Unit = {
    assert(underlying == null)
    val d = items.head.vector.size
    val index = new IndexBuilder(numTrees, d + 2)(distance, random).build(items)
    underlying = new IndexAggregator().prependItems(items).aggregate(index.nodes).result()
  }

  def save(filename: String): Unit = {
    assert(underlying != null)
    val fos = new FileOutputStream(filename)
    underlying.writeAnnoyBinary(f, fos)
    fos.close()
  }

  def load(filename: String): Unit = {
    assert(underlying == null)

    val nodes = new ArrayBuffer[(Int, Node)]
    val buffer = new Array[Byte](12 + f * 4)
    val fis = new FileInputStream(filename)
    val bis = new BufferedInputStream(fis)

    while (bis.read(buffer) > 0) {
      val bf = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN)
      val n = bf.getInt()
      val node = n match {
        case 1 =>
          val norm = bf.getFloat()
          bf.getInt() // skip
          ItemNode(SVector(Array.fill(f)(bf.getFloat())))
        case x if x <= f + 2 =>
          LeafNode(Array.fill(x)(bf.getInt))
        case _ =>
          val l = bf.getInt()
          val r = bf.getInt()
          HyperplaneNode(SVector(Array.fill(f)(bf.getFloat())), l, r)
      }
      nodes += n -> node
      bf.clear()
    }
    bis.close()
    fis.close()

    val nItems = nodes(nodes.length - 1)._1
    val roots = new ArrayBuffer[RootNode]
    var i = nodes.length - 1
    while (i > 0) {
      nodes(i) match {
        case (n, h: HyperplaneNode) if n == nItems =>
          roots += RootNode(i)
        case _ => i = 0
      }
      i -= 1
    }

    if (roots.length > 1 &&
      nodes(roots.head.location)._2.asInstanceOf[HyperplaneNode].l == nodes(roots.last.location)._2.asInstanceOf[HyperplaneNode].l) {
      roots.reduceToSize(roots.length - 1)
    }

    val nodes1 = nodes.map(_._2) ++ roots
    underlying = new Index(nodes1, true)
  }

  def getNItems: Int = items.length

  def getItem(item: Int): Array[Float] = items(item).vector match {
    case SVector(sx) => sx
    case DVector(dx) => dx.map(_.toFloat) // TODO: avoid
  }

  def getNnsByItem(item: Int, n: Int): Array[(Int, Float)] = getNnsByItem(item, n, -1)

  def getNnsByItem(item: Int, n: Int, k: Int): Array[(Int, Float)] = getAllNns(getItem(item), n, k)

  def getNnsByVector(w: Array[Float], n: Int): Array[(Int, Float)] = getNnsByVector(w, n, -1)

  def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)] = getAllNns(w, n, k)

  val ord = new Ordering[(IdVectorWithNorm, Float)]{
    def compare(x: (IdVectorWithNorm, Float), y: (IdVectorWithNorm, Float)): Int = {
      Ordering[Float].compare(x._2, y._2)
    }
  }

  private def getAllNns(v: Array[Float], n: Int, k: Int): Array[(Int, Float)] = {
    val roots = underlying.roots.map { r => underlying.nodes(r.location) }
    val searchK = if (k == -1) n * roots.length else k
    val query = IdVectorWithNorm(-1, v)

    val q = new mutable.PriorityQueue[(Float, Node)]()(Ordering.by(_._1)) ++= roots.map(Float.PositiveInfinity -> _)

    val nns = new ArrayBuffer[IdVectorWithNorm](searchK)
    while (nns.length < searchK && q.nonEmpty) {
      q.dequeue() match {
        case (d, HyperplaneNode(hyperplane, l, r)) =>
          val margin = distance.margin(hyperplane, query.vector)
          underlying.nodes(r) match {
            case ItemNode(vector) => nns += IdVectorWithNorm(r, vector)
            case _ => q += math.min(d, margin.toFloat) -> underlying.nodes(r)
          }
          underlying.nodes(l) match {
            case ItemNode(vector) => nns += IdVectorWithNorm(l, vector)
            case _ => q += math.min(d, -margin.toFloat) -> underlying.nodes(l)
          }
        case (d, FlipNode(l, r)) =>
          underlying.nodes(r) match {
            case ItemNode(vector) => nns += IdVectorWithNorm(r, vector)
            case _ => q += math.min(d, 0) -> underlying.nodes(r)
          }
          underlying.nodes(l) match {
            case ItemNode(vector) => nns += IdVectorWithNorm(l, vector)
            case _ => q += math.min(d, 0) -> underlying.nodes(l)
          }
        case (_, LeafNode(children)) =>
          nns ++= children.map { id =>
            underlying.nodes(id) match {
              case ItemNode(vector) => IdVectorWithNorm(id, vector)
            }
          }
      }
    }

    val boundedQueue = new BoundedPriorityQueue[(IdVectorWithNorm, Float)](n)(ord.reverse)
    nns.distinct foreach { v =>
      boundedQueue += v -> distance.distance(query, v).toFloat
    }

    boundedQueue.toArray.sorted(ord).map(x => x._1.id -> x._2)
  }

}
