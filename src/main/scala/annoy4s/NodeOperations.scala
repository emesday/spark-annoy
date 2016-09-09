package annoy4s

import java.nio.ByteBuffer

/**
  * Created by emeth on 2016. 9. 9..
  */
trait NodeOperations {

  def getNDescendants(underlying: ByteBuffer, offsetInBytes: Int) : Int

  def getChildren(underlying: ByteBuffer, offsetInByte: Int,  i: Int): Int

  def getAllChildren(underlying: ByteBuffer, offsetInByte: Int, dst: Array[Int]): Array[Int]

  def getV(underlying: ByteBuffer, offsetInBytes: Int, dst: Array[Float]): Array[Float]

  def setNDescendants(underlying: ByteBuffer, offsetInBytes: Int, nDescendants: Int): Unit

  def setChildren(underlying: ByteBuffer, offsetInBytes: Int, i: Int, v: Int): Unit

  def setAllChildren(underlying: ByteBuffer, offsetInBytes: Int, indices: Array[Int]): Unit

  def setV(underlying: ByteBuffer, offsetInBytes: Int, v: Array[Float]): Unit

  def setValue(underlying: ByteBuffer, offsetInBytes: Int, v: Float, dim: Float): Unit

  def copy(src: ByteBuffer, srcOffsetInBytes: Int, dst: ByteBuffer, dstOffsetInBytes: Int, nodeSizeInBytes: Int): Unit

}
