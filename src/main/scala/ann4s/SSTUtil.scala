package ann4s

import java.io.File
import java.nio.ByteBuffer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.linalg.{Vector => MlVector}
import org.rocksdb.{EnvOptions, Options, SstFileWriter}

object SSTUtil {

  def writeIndex(index: Index, path: Path, fs: FileSystem): Unit = {
    val localFile = File.createTempFile("sstutil", "index").toString
    println(localFile)
    val envOptions = new EnvOptions
    val options = new Options
    val writer = new SstFileWriter(envOptions, options)
    writer.open(localFile)

    val nodes = index.nodes.zipWithIndex
    nodes foreach { case (node, i) =>
      val key = ByteBuffer.allocate(4).putInt(i).array()
      val value = node.toByteArray
      writer.put(key, value)
    }
    writer.close()
    options.close()
    envOptions.close()
    fs.copyFromLocalFile(true, false, new Path(localFile), path)
  }

  def writeItems(index: Int, it: Iterator[(Int, MlVector, String)], path: Path, fs: FileSystem): Unit = {
    val localFile = File.createTempFile("sstutil", s"items$index").toString
    println(localFile)
    val envOptions = new EnvOptions
    val options = new Options
    val writer = new SstFileWriter(envOptions, options)
    writer.open(localFile)

    it foreach { case (id, vector, metadata) =>
      val metadataBytes = metadata.getBytes("UTF-8")
      val bb = ByteBuffer.allocate(4 + vector.size * 4 + metadataBytes.length)
      bb.putInt(vector.size)
      vector.toArray foreach { x => bb.putFloat(x.toFloat) }
      bb.put(metadataBytes)
      val key = ByteBuffer.allocate(4).putInt(id).array()
      val value = bb.array()
      writer.put(key, value)
    }

    writer.close()
    options.close()
    envOptions.close()
    fs.copyFromLocalFile(true, false, new Path(localFile), path)
  }


}
