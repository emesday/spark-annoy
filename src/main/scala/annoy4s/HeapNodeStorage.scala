package annoy4s

import java.nio.{ByteBuffer, ByteOrder}

class HeapNodeStorage(dim: Int, _size: Int, io: NodeSerde) extends NodeStorage(dim, io) {

  import Functions._

  val reallocation_factor = 1.3

  var size = _size
  var underlying = ByteBuffer.allocate(nodeSizeInBytes * size).order(ByteOrder.LITTLE_ENDIAN)

  override val bufferType: String = underlying.getClass.getSimpleName

  override def getSize: Int = size

  override def numNodes: Int = size / nodeSizeInBytes

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

  override def apply(i: Int): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, io, readonly)

  override def newNode: Node = Node(dim, nodeSizeInBytes,
    ByteBuffer.allocate(nodeSizeInBytes).order(ByteOrder.LITTLE_ENDIAN), 0, io, readonly)

  override def flip(): Unit = underlying.flip()

  def prepareToWrite(): Unit = {
    readonly = true
    underlying.rewind()
  }
}
