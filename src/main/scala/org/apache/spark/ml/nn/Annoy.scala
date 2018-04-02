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

  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(idCol), IntegerType)
    SchemaUtils.checkColumnType(schema, $(featuresCol), new MlVectorUDT)
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
    val fs = FileSystem.get(items.sparkSession.sparkContext.hadoopConfiguration)
    val os = fs.create(new Path(path), true, 1048576)
    val vectorWithIds = items.select($(idCol), $(featuresCol)).rdd.map {
      case Row(id: Int, features: MlVector) =>
        IdVector(id, Vector32(features.toArray.map(_.toFloat)))
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

  setDefault(idCol -> "id", featuresCol -> "features", numTrees -> 1, fraction -> 0.01)

  override def copy(extra: ParamMap): Annoy = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("ann"))

  def setIdCol(value: String): this.type = set(idCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  def setFraction(value: Double): this.type = set(fraction, value)

  override def fit(dataset: Dataset[_]): AnnoyModel = {
    transformSchema(dataset.schema, logging = true)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE

    val instances = dataset.select($(idCol), $(featuresCol)).rdd.map {
      case Row(id: Int, features: MlVector) => IdVectorWithNorm(id, features.toArray.map(_.toFloat))
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
    val globalAggregator = new IndexAggregator
    var i = 0
    while (i < $(numTrees)) {
      val parentTree = new IndexBuilder(1, d + 2).build(samples)
      val localAggregator = new IndexAggregator().aggregate(parentTree.nodes)

      val withSubTreeId = instances.mapPartitionsWithIndex { case (i, it) =>
        // for nodes
        val distance = CosineDistance
        val random = new Random(randomSeed + i + 1)
        it.map(x => parentTree.traverse(x.vector)(distance, random) -> x)
      }
      val grouped = withSubTreeId.groupByKey()

      val subTreeNodesWithId = grouped.mapValues { it =>
        new IndexBuilder(1, d + 2)(CosineDistance, Random).build(it.toIndexedSeq).nodes
      }

      subTreeNodesWithId.collect().foreach { case (subTreeId, subTreeNodes) =>
        localAggregator.mergeSubTree(subTreeId, subTreeNodes)
      }

      globalAggregator.aggregate(localAggregator)
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
