package annoy4s

trait Random {
  def flip(): Boolean
  def index(n: Int): Int
}

object RandRandom extends Random {
  val rnd = new scala.util.Random
  override def flip(): Boolean = rnd.nextBoolean()
  override def index(n: Int): Int = rnd.nextInt(n)
}
