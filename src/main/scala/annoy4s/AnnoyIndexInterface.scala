package annoy4s

trait AnnoyIndexInterface {

  def addItem(item: Int, w: Array[Float]): Unit

  def build(q: Int): Unit

  def save(filename: String): Boolean

  def unload(): Unit

  def load(filename: String): Boolean

  def getDistance(i: Int, j: Int): Float

  def getNnsByItem(item: Int, n: Int, k: Int): Array[(Int, Float)]

  def getNnsByItem(item: Int, n: Int): Array[(Int, Float)] = getNnsByItem(item, n, -1)

  def getNnsByVector(w: Array[Float], n: Int, k: Int): Array[(Int, Float)]

  def getNItems: Int

  def verbose(v: Boolean): Unit

  def getItem(item: Int): Array[Float]

}
