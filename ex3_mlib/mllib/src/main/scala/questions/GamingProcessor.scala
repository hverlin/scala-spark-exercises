package questions

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import org.apache.spark.sql.Row

import org.apache.spark.sql.types.{StructType, StructField, StringType}


/**
  * GamingProcessor is used to predict if the user is a subscriber.
  * You can find the data files from /resources/gaming_data.
  * Data schema is https://github.com/cloudwicklabs/generator/wiki/OSGE-Schema
  * (first table)
  *
  * Use Spark's machine learning library mllib's SVM(support vector machine) algorithm.
  * http://spark.apache.org/docs/1.6.1/mllib-linear-methods.html#linear-support-vector-machines-svms
  *
  * Use these features for training your model:
  *   - gender
  *   - age
  *   - country
  *   - friend_count
  *   - lifetime
  *   - citygame_played
  *   - pictionarygame_played
  *   - scramblegame_played
  *   - snipergame_played
  *
  * -paid_subscriber(this is the feature to predict)
  *
  * The data contains categorical variables, so you need to
  * change them accordingly.
  * https://spark.apache.org/docs/1.6.1/ml-features.html
  *
  *
  *
  */
class GamingProcessor() {


  // these parameters can be changed
  val conf = new SparkConf()
    .setAppName("gaming")
    .setMaster("local[*]")
    .set("spark.driver.memory", "3g")
    .set("spark.executor.memory", "2g")

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._


  /**
    * convert creates a DataFrame, removes unnecessary columns and converts the rest to right format.
    * http://spark.apache.org/docs/1.6.1/sql-programming-guide.html#creating-datasets
    * Data schema:
    *   - gender: Double (1 if male else 0)
    *   - age: Double
    *   - country: String
    *   - friend_count: Double
    *   - lifetime: Double
    *   - game1: Double (citygame_played)
    *   - game2: Double (pictionarygame_played)
    *   - game3: Double (scramblegame_played)
    *   - game4: Double (snipergame_played)
    *   - paid_customer: Double (1 if yes else 0)
    *
    * @param path to file
    * @return converted DataFrame
    */
  def convert(path: String): DataFrame = {

    val schema = StructType(Array(
      StructField("gender", DoubleType, nullable = false),
      StructField("age", DoubleType, nullable = false),
      StructField("country", StringType, nullable = true),
      StructField("friend_count", DoubleType, nullable = false),
      StructField("lifetime", DoubleType, nullable = false),
      StructField("game1", DoubleType, nullable = false),
      StructField("game2", DoubleType, nullable = false),
      StructField("game3", DoubleType, nullable = false),
      StructField("game4", DoubleType, nullable = false),
      StructField("paid_customer", DoubleType, nullable = false))
    )

    val people = sc.textFile(path)
    val rowRDD = people
      .map(_.split(","))
      .map(p => Row(
        if (p(3).equals("male")) 1.0 else 0.0,
        p(4).toDouble,
        p(6),
        p(8).toDouble,
        p(9).toDouble,
        p(10).toDouble,
        p(11).toDouble,
        p(12).toDouble,
        p(13).toDouble,
        if (p(15).equals("yes")) 1.0 else 0.0
      ))
    sqlContext.createDataFrame(rowRDD, schema)
  }

  /**
    * indexer converts categorical variables into doubles.
    * https://en.wikipedia.org/wiki/Categorical_variable
    * https://spark.apache.org/docs/1.6.1/ml-features.html
    * 'country' is the only categorical variable, because
    * it has only 7 possible values (USA,UK,Germany,...).
    * Use some kind of encoder to transform 'country' Strings into
    * 'country_index' Doubles.
    * After these modifications schema should be:
    *
    *   - gender: Double (1 if male else 0)
    *   - age: Double
    *   - country: String
    *   - friend_count: Double
    *   - lifetime: Double
    *   - game1: Double (citygame_played)
    *   - game2: Double (pictionarygame_played)
    *   - game3: Double (scramblegame_played)
    *   - game4: Double (snipergame_played)
    *   - paid_customer: Double (1 if yes else 0)
    *   - country_index: Double
    *
    * @param df DataFrame which has been converted using 'converter'
    * @return Dataframe
    */
  def indexer(df: DataFrame): DataFrame = {
    new StringIndexer()
      .setInputCol("country")
      .setOutputCol("country_index")
      .fit(df).transform(df)
  }

  /**
    * toLabeledPoints converts DataFrame into RDD of LabeledPoints
    * http://spark.apache.org/docs/1.6.1/mllib-data-types.html
    * Label should be 'paid_customer' field and features the rest.
    *
    * To improve performance data also need to be standardized, so use
    * http://spark.apache.org/docs/1.6.1/mllib-feature-extraction.html#standardscaler
    *
    * @param df DataFrame which has been already indexed
    * @return RDD of LabeledPoints
    */
  def toLabeledPoints(df: DataFrame): RDD[LabeledPoint] = {

    val data = df.map(row => LabeledPoint(row.getDouble(9),
      Vectors.dense(
        row.getDouble(0),
        row.getDouble(1),
        row.getDouble(10),
        row.getDouble(3),
        row.getDouble(4),
        row.getDouble(5),
        row.getDouble(6),
        row.getDouble(7),
        row.getDouble(8)
      )))

    val scaler = new StandardScaler().fit(data.map(x => x.features))
    data.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
  }

  /**
    * createModel creates a SVM model
    * When training, 5 iterations should be enough.
    *
    * @param training RDD containing LabeledPoints (created using previous methods)
    * @return trained SVMModel
    */
  def createModel(training: RDD[LabeledPoint]): SVMModel = {
    val svmAlg = new SVMWithSGD()
    svmAlg.optimizer.
      setNumIterations(5).
      setRegParam(0.3).
      setUpdater(new L1Updater)
    svmAlg.run(training)
  }


  /**
    * Given a transformed and normalized dataset
    * this method predicts if the customer is going to
    * subscribe to the service. Predicted values MUST be in the same
    * order as the given RDD.
    *
    * @param model         trained SVM model
    * @param dataToPredict normalized data for prediction (created using previous toLabeledPoints)
    * @return RDD[Double] predicted scores (1.0 == yes, 0.0 == no)
    */
  def predict(model: SVMModel, dataToPredict: RDD[LabeledPoint]): RDD[Double] = {
    dataToPredict.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }.map(row => row._2)
  }

}

/**
  *
  * Change the student id
  */
object GamingProcessor {
  val studentId = "584788"
}
