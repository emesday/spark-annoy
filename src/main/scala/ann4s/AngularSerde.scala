package ann4s

import java.nio.ByteBuffer

/**
  * n_descendants: Int = 4
  * n_children[0]: Int = 4
  * n_children[1]: Int = 4
  * v: Array[Float] = f * 4
  */
trait AngularSerde extends NodeSerde {

  override def nodeSizeInBytes(dim: Int): Int = 12 + dim * 4

  override def childrenCapacity(dim: Int): Int = 2 + dim

  override val offsetDescendants: Int = 0

  override val offsetChildren: Int = 4

  override val offsetValue: Int = 12

  override def getA(underlying: ByteBuffer, offsetInBytes: Int): Float =
    throw new IllegalAccessError()

  override def setA(underlying: ByteBuffer, offsetInBytes: Int, a: Float): Unit =
    throw new IllegalAccessError()

}

