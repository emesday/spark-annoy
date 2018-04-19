package org.apache.spark.ml.nn

import ann4s._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{Vector => MlVector, VectorUDT => MlVectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasSeed}
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import scala.util.Random

trait ANNParams extends Params with HasFeaturesCol with HasSeed {

  final val idCol: Param[String] = new Param[String](this, "idCol", "id column name")

  def getIdCol: String = $(idCol)

  final val numTrees: IntParam = new IntParam(this, "numTrees", "number of trees to build")

  def getNumTrees: Int = $(numTrees)

  final val fraction: DoubleParam = new DoubleParam(this, "fraction", "fraction of data to build parent tree")

  def getFraction: Double = $(fraction)

  final val forAnnoy: BooleanParam = new BooleanParam(this, "forAnnoy", "build Annoy compatible binary")

  def getForAnnoy: Boolean = $(forAnnoy)

  final val maxChildren: IntParam = new IntParam(this, "maxChildren", "max number of children")

  def getMaxChildren: Int = $(maxChildren)

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(idCol), IntegerType)
    SchemaUtils.checkColumnTypes(schema, $(featuresCol),
      Seq(new MlVectorUDT, ArrayType(FloatType, false), ArrayType(FloatType, true)))
    schema
  }
}

trait ANNModelParams extends Params with ANNParams

class AnnoyModel private[ml] (
  override val uid: String,
  val d: Int,
  val index: Index,
  @transient val items: DataFrame
) extends Model[AnnoyModel] with ANNModelParams with MLWritable {

  override def copy(extra: ParamMap): AnnoyModel = {
    copyValues(new AnnoyModel(uid, d, index, items), extra)
  }

  override def transform(dataset: Dataset[_]): DataFrame = throw new Error("not support")

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def write: MLWriter = new AnnoyModel.ANNModelWriter(this)

  def writeAnnoyBinary(path: String): Unit = {
    require($(forAnnoy), "not built for Annoy")
    val fs = FileSystem.get(items.sparkSession.sparkContext.hadoopConfiguration)
    val os = fs.create(new Path(path), true, 1048576)
    val vectorWithIds = items.select($(idCol), $(featuresCol)).rdd.map {
      case Row(id: Int, features: MlVector) =>
        IdVector(id, Vector32(features.toArray.map(_.toFloat)))
      case Row(id: Int, features: Seq[_]) =>
        IdVector(id, Vector32(features.asInstanceOf[Seq[Float]].toArray))
    }
    AnnoyUtil.dump(vectorWithIds.sortBy(_.id).toLocalIterator, index.getNodes, os)
    os.close()
  }

}

object AnnoyModel extends MLReadable[AnnoyModel] {

  override def read: MLReader[AnnoyModel] = new ANNModelReader

  override def load(path: String): AnnoyModel = super.load(path)

  private[AnnoyModel] class ANNModelWriter(instance: AnnoyModel) extends MLWriter {
    override protected def saveImpl(path: String): Unit = {
      NodeUDT.register()
      val extraMetadata = "d" -> instance.d
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMetadata))
      val indexPath = new Path(path, "index").toString
      val itemPath = new Path(path, "items").toString
      val data = instance.index.getNodes
      sparkSession.createDataFrame(Array(data)).repartition(1).write.parquet(indexPath)
      instance.items.write.parquet(itemPath)
    }
  }

  private class ANNModelReader extends MLReader[AnnoyModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[AnnoyModel].getName

    override def load(path: String): AnnoyModel = {
      NodeUDT.register()
      val sparkSession = super.sparkSession
      import sparkSession.implicits._
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      implicit val format = DefaultFormats
      val d = (metadata.metadata \ "d").extract[Int]
      val treePath = new Path(path, "index").toString
      val itemPath = new Path(path, "items").toString
      val forest = sparkSession.read.parquet(treePath).as[Nodes].head()
      val items = sparkSession.read.parquet(itemPath)
      val model = new AnnoyModel(metadata.uid, d, forest.toIndex, items)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

class Annoy(override val uid: String)
  extends Estimator[AnnoyModel] with ANNParams with DefaultParamsWritable {

  setDefault(idCol -> "id", featuresCol -> "features", numTrees -> 1, fraction -> 0.01,
    forAnnoy -> true, maxChildren -> 0)

  override def copy(extra: ParamMap): Annoy = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("ann"))

  def setIdCol(value: String): this.type = set(idCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  def setFraction(value: Double): this.type = set(fraction, value)

  def setForAnnoy(value: Boolean): this.type = set(forAnnoy, value)

  def setMaxChildren(value: Int): this.type = set(maxChildren, value)

  override def fit(dataset: Dataset[_]): AnnoyModel = {
    transformSchema(dataset.schema, logging = true)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE

    val instances = dataset.select($(idCol), $(featuresCol)).rdd.map {
      case Row(id: Int, features: MlVector) =>
        IdVectorWithNorm(id, features.toArray.map(_.toFloat))
      case Row(id: Int, features: Seq[_]) =>
        IdVectorWithNorm(id, features.asInstanceOf[Seq[Float]].toArray)
    }

    if (handlePersistence) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    val instr = Instrumentation.create(this, instances)
    instr.logParams(numTrees, fraction, seed)

    // for local
    val randomSeed = $(seed)
    implicit val distance: Distance = CosineDistance
    implicit val localRandom: Random = new Random(randomSeed)

    val samples = instances.sample(withReplacement = false, $(fraction), localRandom.nextLong()).collect()
    val d = samples.head.vector.size
    val mc = if ($(forAnnoy) || $(maxChildren) == 0) d + 2 else $(maxChildren)
    val parentTreeMc = math.max(mc, samples.length / instances.getNumPartitions)

    logDebug(s"numSamples: ${samples.length}, d: $d, maxChildren: $mc, parentTreeMaxChildren: $parentTreeMc")

    val globalAggregator = new IndexAggregator
    var i = 0
    while (i < $(numTrees)) {
      logDebug(s"building tree ${i + 1}/${$(numTrees)}")
      val parentTree = new IndexBuilder(1, parentTreeMc, needLeafNode = false).build(samples)
      logDebug("parent tree was built")
      val localAggregator = new IndexAggregator().aggregate(parentTree.nodes)
      logDebug("parent tree was aggregated")

      val bcParentTree = instances.sparkContext.broadcast(parentTree)
      logDebug("parent tree was broadcasted")
      val withSubTreeId = instances.mapPartitionsWithIndex { case (i, it) =>
        // for nodes
        val distance = CosineDistance
        val random = new Random(randomSeed + i + 1)
        it.map(x => bcParentTree.value.traverse(x.vector)(distance, random) -> x)
      }

      val grouped = withSubTreeId.groupByKey()

      val subTreeNodesWithId = grouped.mapValues { it =>
        new IndexBuilder(1, mc)(CosineDistance, Random).build(it.toIndexedSeq).nodes
      }

      logDebug("collect() invokes the sub jobs simultaneously")
      subTreeNodesWithId.collect().foreach { case (subTreeId, subTreeNodes) =>
        logDebug(s"aggregating subTree: $subTreeId, nodes: ${subTreeNodes.length}")
        localAggregator.mergeSubTree(subTreeId, subTreeNodes)
      }
      logDebug("sub trees were aggregated to localAggregator")
      globalAggregator.aggregate(localAggregator)
      logDebug("localAggregator was merged to globalAggregator")
      bcParentTree.unpersist()
      i += 1
    }

    val index = globalAggregator.result()

    val items = dataset.select($(idCol), $(featuresCol))
    val model = copyValues(new AnnoyModel(uid, d, index, items)).setParent(this)
    instr.logSuccess(model)
    if (handlePersistence) {
      instances.unpersist()
    }
    model
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object Annoy extends DefaultParamsReadable[Annoy] {
  override def load(path: String): Annoy = super.load(path)
}
