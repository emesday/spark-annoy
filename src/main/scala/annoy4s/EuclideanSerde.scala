package annoy4s

import java.nio.ByteBuffer

/**
  * n_descendants: Int = 4
  * a: Float = 4
  * n_children[0]: Int = 4
  * n_children[1]: Int = 4
  * v: Array[Float] = f * 4
  */
trait EuclideanSerde extends NodeSerde {

  override def nodeSizeInBytes(dim: Int): Int = 16 + dim * 4
  override def childrenCapacity(dim: Int): Int = 2 + dim
  override val offsetDescendants: Int = 0
  override val offsetChildren: Int = 8
  override val offsetValue: Int = 16

  val offsetA = 4

  override def getA(underlying: ByteBuffer, offsetInBytes: Int): Float = {
    underlying.position(offsetInBytes + offsetA)
    underlying.getFloat()
  }

  override def setA(underlying: ByteBuffer, offsetInBytes: Int, a: Float): Unit = {
    underlying.position(offsetInBytes + offsetA)
    underlying.putFloat(a)
  }
}
