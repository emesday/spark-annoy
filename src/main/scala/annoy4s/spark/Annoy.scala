package annoy4s.spark

import annoy4s.{FixRandom, RandRandom, Random, AnnoyIndex}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{Identifiable, MLWritable, MLWriter}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

trait AnnoyModelParams extends Params {

  val idCol: Param[String] = new Param[String](this, "idCol", "id column name")

  setDefault(idCol, "id")

  def getIdCol: String = $(idCol)

  val featuresCol: Param[String] = new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  def getFeaturesCol: String = $(featuresCol)

  val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  def getOutputCol: String = $(outputCol)

  setDefault(outputCol, "neighbors")

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
    StructType(schema :+ StructField($(outputCol), IntegerType))
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val getNns = udf { (features: Seq[Float]) =>
      if (features != null) {
        annoyIndexOnExecutor.getNnsByVector(features.toArray, $(k), -1).map(_._1)
      } else {
        Array.empty[Int]
      }
    }

    dataset
      .select(dataset("*"),
        getNns(dataset($(featuresCol))).as($(outputCol)))
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

  def setOutputCol(value: String): this.type = set(outputCol, value)

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
    StructType(schema :+ StructField($(outputCol), IntegerType))
  }
}

