package ann4s

import java.io.RandomAccessFile
import java.nio.ByteOrder
import java.nio.channels.FileChannel

import scala.collection.mutable

class MappedNodeStorage(dim: Int, filename: String, io: NodeStruct) extends NodeStorage(dim, io) {

  val memoryMappedFile = new RandomAccessFile(filename, "r")

  private val fileSize = memoryMappedFile.length().toInt

  val underlying = memoryMappedFile.getChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize)
    .order(ByteOrder.LITTLE_ENDIAN)

  private var size = fileSize

  var idToIndex: mutable.OpenHashMap[String, Int] = mutable.OpenHashMap.empty[String, Int]

  val versionBytes = new Array[Byte](4)
  underlying.position(fileSize - 4)
  underlying.get(versionBytes)
  new String(versionBytes, "UTF-8") match {
    case "v001" =>
      underlying.position(fileSize - 8)
      val last = underlying.getInt
      underlying.position(last)
      val numIds = underlying.getInt
      idToIndex = new mutable.OpenHashMap[String, Int](numIds)
      (0 until numIds) foreach { i =>
        val length = underlying.getInt
        val bytes = new Array[Byte](length)
        underlying.get(bytes)
        idToIndex.update(new String(bytes, "UTF-8"), i)
      }
      underlying.limit(last)
      size = last
    case _ =>
  }

  override val bufferType = underlying.getClass.getSimpleName

  override def getSize: Int = size

  override def numNodes: Int = size / nodeSizeInBytes

  override def ensureSize(n: Int, verbose: Boolean): Int = throw new IllegalAccessError("readonly")

  override def newNode: Node = throw new IllegalAccessError("readonly")

  override def apply(i: Int, _readonly: Boolean): Node = Node(dim, nodeSizeInBytes, underlying, i * nodeSizeInBytes, io, true)

  override def flip(): Unit = throw new IllegalAccessError("readonly")

  def close() = memoryMappedFile.close()
}
