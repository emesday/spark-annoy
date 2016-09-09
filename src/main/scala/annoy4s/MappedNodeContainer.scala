package annoy4s

import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel

class MappedNodeContainer[T <: NodeOperations](dim: Int, filename: String) extends NodeContainer {

  val memoryMappedFile = new RandomAccessFile(filename, "r")
  val fileSize = memoryMappedFile.length()
  val underlying = memoryMappedFile.getChannel.map(
    FileChannel.MapMode.READ_ONLY, 0, fileSize)
    .order(ByteOrder.LITTLE_ENDIAN)

  val (nodeSizeInBytes, childrenCapacity, ops) =  {
    //    case cls if classOf[AngularNode].isAssignableFrom(cls) =>
    (AngularNodeOperations.nodeSizeInBytes(dim), AngularNodeOperations.childrenCapacity(dim), AngularNodeOperations)
  }

  override val bufferType = underlying.getClass.getSimpleName

  override def getSize: Int = fileSize.toInt

  override def ensureSize(n: Int, verbose: Boolean): Int = throw new IllegalAccessError("readonly")

  override def newNode: Node = throw new IllegalAccessError("readonly")

  override def apply(i: Int): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, ops, true)

  override def flip(): Unit = throw new IllegalAccessError("readonly")

  def close() = {
    memoryMappedFile.close()
  }
}
