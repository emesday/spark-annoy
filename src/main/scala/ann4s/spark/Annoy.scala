package ann4s.spark

import ann4s.{Angular, AnnoyIndex, Euclidean, FixRandom}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.proxy.{DefaultParamsReader, DefaultParamsWriter}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

trait AnnoyModelParams extends Params {

  val idCol: Param[String] = new Param[String](this, "idCol", "id column name")

  setDefault(idCol, "id")

  def getIdCol: String = $(idCol)

  val featuresCol: Param[String] = new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  def getFeaturesCol: String = $(featuresCol)

  val neighborCol: Param[String] = new Param[String](this, "neighborCol", "neighbor column name")

  def getNeighborCol: String = $(neighborCol)

  setDefault(neighborCol, "neighbor")

  val distanceCol: Param[String] = new Param[String](this, "distanceCol", "distance column name")

  def getDistanceCol: String = $(distanceCol)

  setDefault(distanceCol, "distance")

  def k: IntParam = new IntParam(this, "k", "number of neighbors to find")

  setDefault(k, 10)

  def getK: Int = $(k)

  val debug: BooleanParam = new BooleanParam(this, "debug", "set on/off debug mode")

  setDefault(debug, false)

  def getDebug: Boolean = $(debug)

}

class AnnoyModel (
  override val uid: String,
  val dimension: Int,
  private val indexFile: String)
  extends Model[AnnoyModel] with AnnoyModelParams with MLWritable {

  def setK(value: Int): this.type = set(k, value)

  // every executors initialize this respectively.
  private var _annoyIndexOnExecutor: AnnoyIndex = _

  private def annoyIndexOnExecutor: AnnoyIndex = if (_annoyIndexOnExecutor == null) {
    _annoyIndexOnExecutor = if ($(debug)) {
      new AnnoyIndex(dimension, FixRandom)
    } else {
      new AnnoyIndex(dimension)
    }

    _annoyIndexOnExecutor.load(indexFile)
    _annoyIndexOnExecutor
  } else {
    _annoyIndexOnExecutor
  }

  override def copy(extra: ParamMap): AnnoyModel = {
    val copied = new AnnoyModel(uid, dimension, indexFile)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new AnnoyModel.AnnoyModelWriter(this)

  override def transformSchema(schema: StructType): StructType = {
    // |id|neighbor|distance|
    StructType(Seq(
      StructField($(idCol), IntegerType),
      StructField($(neighborCol), IntegerType),
      StructField($(distanceCol), FloatType)
    ))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    // broadcast the file
    dataset.sqlContext.sparkContext.addFile(indexFile)

    val getNns = udf { (features: Seq[Float]) =>
      if (features != null) {
        annoyIndexOnExecutor.getNnsByVector(features.toArray, $(k), -1)
      } else {
        Array.empty[(Int, Float)]
      }
    }

    dataset
      .withColumn("nns", explode(getNns(dataset($(featuresCol)))))
      .select(dataset($(idCol)),
        col("nns")("_1") as $(neighborCol),
        col("nns")("_2") as $(distanceCol))
  }

}

object AnnoyModel extends MLReadable[AnnoyModel] {

  override def read: MLReader[AnnoyModel] = new AnnoyModelReader

  override def load(path: String): AnnoyModel = super.load(path)

  private[AnnoyModel] class AnnoyModelWriter(instance: AnnoyModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val extraMap = ("dimension" -> instance.dimension) ~
        ("indexFile" -> instance.indexFile)
      DefaultParamsWriter.saveMetadata(instance, path, sc, Some(extraMap))
      FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
        .copyFromLocalFile(false, new Path(instance.indexFile), new Path(path, instance.indexFile))
    }

  }

  private class AnnoyModelReader extends MLReader[AnnoyModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[AnnoyModel].getName

    override def load(path: String): AnnoyModel = {
      implicit val format = DefaultFormats
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dimension = (metadata.metadata \ "dimension").extract[Long].toInt
      val indexFile = (metadata.metadata \ "indexFile").extract[String]

      FileSystem.get(sqlContext.sparkContext.hadoopConfiguration)
        .copyToLocalFile(false, new Path(path, indexFile), new Path(indexFile))

      val model = new AnnoyModel(metadata.uid, dimension, indexFile)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

trait AnnoyParams extends AnnoyModelParams {

  def metricValidator: String => Boolean = {
    case "angular" | "euclidean" => true
    case _ => false
  }

  val dimension: IntParam = new IntParam(this, "dimension", "dimension of vectors", ParamValidators.gtEq(1))

  def getDimension: Int = $(dimension)

  val metric: Param[String] = new Param[String](this, "metric", "metric", metricValidator)

  setDefault(metric, "angular")

  def getMetric: String = $(metric)

  val numTrees: IntParam = new IntParam(this, "numTrees", "number of trees", ParamValidators.gtEq(1))

  setDefault(numTrees, 10)

  def getNumTrees: Int = $(numTrees)

}

class Annoy(override val uid: String) extends Estimator[AnnoyModel] with AnnoyParams {

  def setIdCol(value: String): this.type = set(idCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setDimension(value: Int): this.type = set(dimension, value)

  def setNeighborCol(value: String): this.type = set(neighborCol, value)

  def setDistanceCol(value: String): this.type = set(distanceCol, value)

  def setDebug(value: Boolean): this.type = set(debug, value)

  def setNumTrees(value: Int): this.type = set(numTrees, value)

  def setMetric(value: String): this.type = set(metric, value)

  def this() = this(Identifiable.randomUID("annoy"))

  override def fit(dataset: DataFrame): AnnoyModel = {
    val annoyOutputFile = s"annoy-index-$uid"

    val m = if ($(metric) == "angular") {
      Angular
    } else if ($(metric) == "euclidean") {
      Euclidean
    } else {
      throw new IllegalArgumentException()
    }

    val annoyIndex = if ($(debug)) {
      new AnnoyIndex($(dimension), m, FixRandom)
    } else {
      new AnnoyIndex($(dimension), m)
    }

    val items = dataset
      .select($(idCol), $(featuresCol))
      .map { case Row(id: Int, features: Seq[_]) =>
        (id, features.asInstanceOf[Seq[Float]].toArray)
      }

    items.toLocalIterator
      .foreach { case (id, features) =>
        annoyIndex.addItem(id, features)
      }

    annoyIndex.build($(numTrees))
    annoyIndex.save(annoyOutputFile)
    annoyIndex.unload()

    val model = new AnnoyModel(uid, $(dimension), annoyOutputFile).setParent(this)
    copyValues(model)
  }

  override def copy(extra: ParamMap): Estimator[AnnoyModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    // |id|neighbor|distance|
    StructType(Seq(
      StructField($(idCol), IntegerType),
      StructField($(neighborCol), IntegerType),
      StructField($(distanceCol), FloatType)
    ))
  }

}

