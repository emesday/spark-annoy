package annoy4s

import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel

class MappedNodeStorage(dim: Int, filename: String, io: NodeSerde) extends NodeStorage(dim, io) {

  val memoryMappedFile = new RandomAccessFile(filename, "r")
  val fileSize = memoryMappedFile.length()
  val underlying = memoryMappedFile.getChannel.map(
    FileChannel.MapMode.READ_ONLY, 0, fileSize)
    .order(ByteOrder.LITTLE_ENDIAN)

  override val bufferType = underlying.getClass.getSimpleName

  override def getSize: Int = fileSize.toInt

  override def ensureSize(n: Int, verbose: Boolean): Int = throw new IllegalAccessError("readonly")

  override def newNode: Node = throw new IllegalAccessError("readonly")

  override def apply(i: Int): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, io, true)

  override def flip(): Unit = throw new IllegalAccessError("readonly")

  def close() = {
    memoryMappedFile.close()
  }
}
