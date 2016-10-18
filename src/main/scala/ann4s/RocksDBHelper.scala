package ann4s

import java.io.File
import java.util

import org.rocksdb._
import scala.collection.JavaConversions._

object RocksDBHelper {

  def using[A, B <: { def close(): Unit }](closeable: B)(f: B => A): A = {
    try {
      f(closeable)
    } finally {
      if (closeable != null) {
        closeable.close()
      }
    }
  }

}

class RocksDBHelper(dbPath: String) {

  import RocksDBHelper._

  RocksDB.loadLibrary()

  val logPath = "/tmp/ann4s/logs"

  private val writeBufferSize = 1024 * 1024 * 512

  private val arenaBlockSize = 2014 *32

  private val dbOptions = new DBOptions()
    .createStatistics()
    .setDbLogDir(logPath)
    .setTableCacheNumshardbits(5)
    .setIncreaseParallelism(8)
    .setAllowOsBuffer(true)
    .setAllowMmapReads(true)

  private val readOptions = new ReadOptions().setFillCache(true).setVerifyChecksums(false)

  private val writeOptions = new WriteOptions()

  private val numItemsKey = "numItems".getBytes

  private val columnFamilies = Seq(
    //default
    ("default", "max"),
    //atomicIndex: Int -> feat: Array[Float]
    ("item", null),
    // atomicIndex: Int -> metadata: String
    ("metadata", null),
    // id: String -> atomicIndex: Int
    ("idmap", null),
    // atomicIndex: Int -> treeNode: TreeNode
    ("node", null)
  )

  val columnFamilyDescriptors: Seq[ColumnFamilyDescriptor] = columnFamilies.map {
    case (name, mergeOperatorName) if mergeOperatorName != null =>
      new ColumnFamilyDescriptor(name.getBytes, new ColumnFamilyOptions().setMergeOperatorName(mergeOperatorName).setWriteBufferSize(writeBufferSize).setArenaBlockSize(arenaBlockSize))
    case (name, mergeOperatorName) =>
      new ColumnFamilyDescriptor(name.getBytes, new ColumnFamilyOptions().setWriteBufferSize(writeBufferSize).setArenaBlockSize(arenaBlockSize))
  }

  def initializeDB(): Unit = {
    if (! new File(dbPath).exists()) {
      println("initializing db ...")
      using(RocksDB.open(new Options().setCreateIfMissing(true), dbPath)) { db =>
        columnFamilyDescriptors
          .filter(d => new String(d.columnFamilyName()) != "default")
          .foreach { d =>
            println("    creating columnFamily:" + new String(d.columnFamilyName()))
            using(db.createColumnFamily(d)) { _ => }
          }
        using(db.getDefaultColumnFamily) { handle =>
          db.put(handle, numItemsKey, Bytes.int2bytes(0))
        }
        println("done")
      }
    }
  }

  def openDB(): (RocksDB, Map[String, ColumnFamilyHandle]) = {
    val handles = new util.ArrayList[ColumnFamilyHandle]()
    val db = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, handles)
    (db, columnFamilyDescriptors.zip(handles).map { case (key, value) => (new String(key.columnFamilyName()), value) }.toMap)
  }

  def getNumItems: Int = {
    Bytes.bytes2int(db.get(handles("default"), numItemsKey), 0)
  }

  def mergeNumItems(numItem: Int): Unit = {
    db.merge(handles("default"), numItemsKey, Bytes.int2bytes(numItem))
  }

  private def usingBatch(block: WriteBatch => Unit): Unit = {
    using(new WriteBatch) { batch =>
      block(batch)
      db.write(writeOptions, batch)
    }
  }

  def exists(id: String): Boolean = {
    db.get(handles("idmap"), id.getBytes) != null
  }

  def getAtomicIndex(id: String): Int = {
    val bytes = db.get(handles("idmap"), id.getBytes)
    if (bytes == null) {
      -1
    } else {
      Bytes.bytes2int(bytes, 0)
    }
  }

  def putAll(id: String, atomicIndex: Int, feat: Array[Float], metadata: String): Unit = {
    usingBatch { batch =>
      println(s"id: $id, atomicIndex: $atomicIndex, feat: $feat, metadata: $metadata")
      val atomicIndexBytes = Bytes.int2bytes(atomicIndex)
      batch.merge(handles("default"), numItemsKey, Bytes.int2bytes(atomicIndex + 1))
      batch.put(handles("idmap"), id.getBytes, atomicIndexBytes)
      batch.put(handles("item"), atomicIndexBytes, Bytes.floats2bytes(feat))
      batch.put(handles("metadata"), atomicIndexBytes, metadata.getBytes)
    }
  }

  def putRoot(atomicNodeIndex: Int): Unit = {
    db.put(handles("node"), writeOptions, f"root_$atomicNodeIndex%010d".getBytes, Bytes.int2bytes(atomicNodeIndex))
  }

  def getNode(i: Int, dim: Int): (Array[Int], Array[Float]) = {
    val node = db.get(handles("node"), readOptions, Bytes.int2bytes(i))
    if (node(0) == 'l') {
      (Bytes.bytes2ints(node, 1, -1), null)
    } else if (node(0) == 'h') {
      (Bytes.bytes2ints(node, 1, 2), Bytes.bytes2floats(node, 1 + 4 + 4, dim))
    } else {
      throw new IllegalArgumentException
    }
  }

  def putLeafNode(atomicNodeIndex: Int, children: Array[Int]): Unit = {
    db.put(handles("node"), writeOptions, Bytes.int2bytes(atomicNodeIndex), Array[Byte]('l') ++ Bytes.ints2bytes(children))
  }

  def putHyperplaneNode(atomicNodeIndex: Int, children: Array[Int], hyperplane: Array[Float]) = {
    db.put(handles("node"), writeOptions, Bytes.int2bytes(atomicNodeIndex), Array[Byte]('h') ++ Bytes.ints2bytes(children) ++ Bytes.floats2bytes(hyperplane))
  }

  def getFeat(i: Int, feat: Array[Float]): Array[Float] = {
    val bytes = db.get(handles("item"), readOptions, Bytes.int2bytes(i))
    Bytes.bytes2floats(bytes, 0, feat)
  }

  def getFeat(i: Int, dim: Int): Array[Float] = {
    val bytes = db.get(handles("item"), readOptions, Bytes.int2bytes(i))
    Bytes.bytes2floats(bytes, 0, dim)
  }

  def close(): Unit = {
    handles.foreach(_._2.close())
    db.close()
  }

  initializeDB()

  val (db, handles) = openDB()

}

/*
class Index(dbPath: String) {

  def add(id: String, feat: Array[Float], metadata: String): Unit = {
    val threadId = Thread.currentThread().getId

    if (db.get(idmapColumnFamily, id.getBytes()) == null) {
      val atomicIndexId = atomicInteger.getAndIncrement()
      println(s"$id:$atomicIndexId: [$threadId] add a new item")
      Thread.sleep(scala.util.Random.nextInt(1000 - id.toInt))
      val atomicIndexIdBytes = f"$atomicIndexId%010d".getBytes
      println(s"$id:$atomicIndexId a: ${new String(db.get(defaultColumnFamily, Keys.numItems))}")
      val wb = new WriteBatch()
      wb.merge(defaultColumnFamily, Keys.numItems, f"${atomicIndexId + 1}%010d".getBytes)
      println(s"$id:$atomicIndexId merge $atomicIndexId")
      wb.put(idmapColumnFamily, id.getBytes(), atomicIndexIdBytes)
      wb.put(itemColumnFamily, atomicIndexIdBytes, Bytes.fromFloatArray(feat))
      wb.put(metadataColumnFamily, atomicIndexIdBytes, metadata.getBytes())
      db.write(new WriteOptions, wb)
      println(s"$id:$atomicIndexId b: ${new String(db.get(defaultColumnFamily, Keys.numItems))}")
    } else {
      println(s"$id skip a new item")
    }

    RocksDB
  }

  def test(): Unit = {
    println(s"current atomicInteger: $atomicInteger")
    println(s"current numItems: ${new String(db.get(Keys.numItems))}")

  }

  //  def getFeat(id: String): Array[Float]
  //
  //  def getNumItems: Int
  //
  //  def getRandomId: String
  //
  //  def getMetadata(id: String): String
  //
  //  def addTree(): Unit
  //
  //  def search(feat: Array[Float]): Array[(String, Float)]
  //
  def close(): Unit = {
    db.close()
    options.close()
    println("closed")
  }

}

object InitDB {

  def main(args: Array[String]) {


  }

}
*/

