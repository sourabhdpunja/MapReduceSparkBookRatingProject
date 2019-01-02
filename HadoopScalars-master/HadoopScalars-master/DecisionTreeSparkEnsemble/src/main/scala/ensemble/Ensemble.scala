package ensemble

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.LogManager
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.functions._

object Ensemble {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\ntw.Ensemble <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Ensemble")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val spark = SparkSession
      .builder()
      .appName("Follower count")
      .getOrCreate()

    import spark.implicits._


    val dfBookRatings = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").load(args(0)+"/BX-Book-Ratings.csv");
    var filteredBookRatings = dfBookRatings.filter($"Book-Rating" !== 0);

    val dfBooks = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").load(args(0)+"/BX-Books.csv");

    val dfUsers = spark.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").load(args(0)+"/BX-Users.csv");

    // Data Filtering from Books
    import org.apache.spark.sql
    val droppedBooks = dfBooks.drop("Image-URL-S").drop("Image-URL-M").drop("Image-URL-L")
    val avgyear = droppedBooks.select(avg($"Year-Of-Publication")).withColumn("avg-year",$"avg(Year-Of-Publication)".cast(sql.types.DataTypes.IntegerType)).first().getInt(1);

    var filteredBooks = droppedBooks.withColumn("Year-Of-Publication", when($"Year-Of-Publication" === "DK Publishing Inc", avgyear.toString).otherwise($"Year-Of-Publication"));
    filteredBooks = filteredBooks.withColumn("Year-Of-Publication", when($"Year-Of-Publication" === 0, avgyear.toString).otherwise($"Year-Of-Publication"));
    filteredBooks = filteredBooks.withColumn("Year-Of-Publication", when($"Year-Of-Publication" >= 2005, avgyear.toString).otherwise($"Year-Of-Publication"));
    filteredBooks = filteredBooks.withColumn("Publisher", when($"Publisher".isNull, "other").otherwise($"Publisher"));

    var filteredUsers = dfUsers.withColumn("Age",
      when($"Age" <= 5, avgyear.toString)
        .when($"Age" >= 90, avgyear.toString)
        .when($"Age".isNull, avgyear.toString)
        .otherwise($"Age"));
    var joined_book_data = filteredBookRatings.join(filteredBooks, "ISBN").join(filteredUsers, "User-ID");
    joined_book_data = joined_book_data.sample(true, 1)

    val splits = joined_book_data.randomSplit(Array(0.9, 0.1))
    val (trainingData, testData) = (splits(0), splits(1))
    trainingData.persist();
    testData.persist();

    val yearOfPublicationIndexer = new StringIndexer()
      .setInputCol("Year-Of-Publication")
      .setOutputCol("Index-Year")
      .setHandleInvalid("keep")

    val ageIndexer = new StringIndexer()
      .setInputCol("Age")
      .setOutputCol("Index-Age")
      .setHandleInvalid("keep")

    val locationIndexer = new StringIndexer()
      .setInputCol("Location")
      .setOutputCol("Index-Location")
      .setHandleInvalid("keep")

    val isbnIndexer = new StringIndexer()
      .setInputCol("ISBN")
      .setOutputCol("Index-ISBN")
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(Array("Index-Age", "Index-Year", "Index-ISBN", "Index-Location"))
      .setOutputCol("indexedFeatures")

    val decisionTreeRegressor = new DecisionTreeRegressor()
      .setLabelCol("Book-Rating")
      .setFeaturesCol("indexedFeatures")
      .setPredictionCol("Predicted Book-Rating")
      .setMaxBins(290788)
      .setMaxDepth(20)
      .setImpurity("variance")

    val pipeline = new Pipeline().setStages(
      Array(ageIndexer, yearOfPublicationIndexer, locationIndexer, isbnIndexer, assembler, decisionTreeRegressor))


    val model = pipeline.fit(trainingData)

    model.save(args(1)+"_Model")

    // Make predictions.
    val predictions = model.transform(testData)
    //
    predictions.select("Book-Title", "Book-Author","Predicted Book-Rating",  "indexedFeatures", "Book-Rating").rdd.map(_.toString()).saveAsTextFile(args(1) + "_predictions")


    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("Book-Rating")
      .setPredictionCol("Predicted Book-Rating")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    logger.info("Root Mean Squared Error (RMSE) on test data =" + rmse)
    val rmseoutput = sc.parallelize(Seq(("Root Mean Squared Error (RMSE) on test data = ", rmse)))

    rmseoutput.saveAsTextFile(args(1)+ "_RMSE")

  }
}