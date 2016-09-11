package ann4s.spark

import ann4s.{AnnoyIndex, FixRandom}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

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
  var _annoyIndexOnExecutor: AnnoyIndex = _

  def annoyIndexOnExecutor: AnnoyIndex = if (_annoyIndexOnExecutor == null) {
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

  override def write: MLWriter = ???

  override def transformSchema(schema: StructType): StructType = {
    // |id|neighbor|distance|
    StructType(Seq(
      StructField($(idCol), IntegerType),
      StructField($(neighborCol), IntegerType),
      StructField($(distanceCol), FloatType)
    ))
  }

  override def transform(dataset: DataFrame): DataFrame = {
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

trait AnnoyParams extends AnnoyModelParams {

  val dimension = new IntParam(this, "dimension", "dimension of vectors", ParamValidators.gtEq(1))

  def getDimension: Int = $(dimension)

}

class Annoy(override val uid: String) extends Estimator[AnnoyModel] with AnnoyParams {

  def setIdCol(value: String): this.type = set(idCol, value)

  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  def setDimension(value: Int): this.type = set(dimension, value)

  def setNeighborCol(value: String): this.type = set(neighborCol, value)

  def setDistanceCol(value: String): this.type = set(distanceCol, value)

  def setDebug(value: Boolean): this.type = set(debug, value)

  def this() = this(Identifiable.randomUID("annoy"))

  override def fit(dataset: DataFrame): AnnoyModel = {
    val annoyOutputFile = s"annoy-index-$uid"
    val annoyIndex = if ($(debug)) {
      new AnnoyIndex($(dimension), FixRandom)
    } else {
      new AnnoyIndex($(dimension))
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
    annoyIndex.build(10)
    annoyIndex.save(annoyOutputFile)
    annoyIndex.unload()

    // broadcast the file
    dataset.sqlContext.sparkContext.addFile(annoyOutputFile)

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

