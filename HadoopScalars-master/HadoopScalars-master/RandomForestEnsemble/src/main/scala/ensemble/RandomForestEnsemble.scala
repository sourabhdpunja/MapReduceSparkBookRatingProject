package ensemble

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.{DecisionTreeRegressor, RandomForestRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RandomForestEnsemble {

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
    val avgyear = dfBooks.select(avg($"Year-Of-Publication")).withColumn("avg-year",$"avg(Year-Of-Publication)".cast(sql.types.DataTypes.IntegerType)).first().getInt(1);


    var filteredBooks = droppedBooks.withColumn("Year-Of-Publication", when($"Year-Of-Publication" === "DK Publishing Inc", avgyear.toString).otherwise($"Year-Of-Publication"));
    filteredBooks = filteredBooks.withColumn("Year-Of-Publication", when($"Year-Of-Publication" === 0, avgyear.toString).otherwise($"Year-Of-Publication"));
    filteredBooks = filteredBooks.withColumn("Year-Of-Publication", when($"Year-Of-Publication" >= 2005, avgyear.toString).otherwise($"Year-Of-Publication"));
    filteredBooks = filteredBooks.withColumn("Publisher", when($"Publisher".isNull, "other").otherwise($"Publisher"));

    // var filteredUsers = dfUsers.filter($"Age" >= 5 || $"Age" <= 90).filter(!$"Age".isNull);
    var filteredUsers = dfUsers.withColumn("Age",
     when($"Age" <= 5, avgyear.toString)
       .when($"Age" >= 90, avgyear.toString)
       .when($"Age".isNull, avgyear.toString)
       .otherwise($"Age"));

    var joined_book_data = filteredBookRatings.join(filteredBooks, "ISBN").join(filteredUsers, "User-ID");
//
    // joined_book_data.printSchema();
    // joined_book_data = joined_book_data.sample(true, 0.75)
    joined_book_data.withColumn("User-ID", $"User-ID".cast(sql.types.DataTypes.StringType))
    joined_book_data.printSchema()

    joined_book_data = joined_book_data.sample(true, 0.9)
    
    val splits = joined_book_data.randomSplit(Array(0.9, 0.1))
    val (trainingData, testData) = (splits(0), splits(1))

    trainingData.persist();
    testData.persist();

    val yearOfPublicationIndexer = new StringIndexer()
      .setInputCol("Year-Of-Publication")
      .setOutputCol("Index-Year")
      .setHandleInvalid("keep")

    val locationIndexer = new StringIndexer()
      .setInputCol("Location")
      .setOutputCol("Index-Location")
      .setHandleInvalid("keep")

    val publisherIndexer = new StringIndexer()
      .setInputCol("publisher")
      .setOutputCol("Index-publisher")
      .setHandleInvalid("keep")

    val ageIndexer = new StringIndexer()
      .setInputCol("Age")
      .setOutputCol("Index-Age")
      .setHandleInvalid("keep")

    val assembler = new VectorAssembler()
      .setInputCols(Array("Index-Age", "Index-publisher", "Index-Location", "Index-Year"))
      .setOutputCol("indexedFeatures")

    val randomForestRegressor = new RandomForestRegressor()
      .setLabelCol("Book-Rating")
      .setFeaturesCol("indexedFeatures")
      .setPredictionCol("Predicted Book-Rating")
      .setMaxBins(290788)
      .setMaxDepth(20)
      .setImpurity("variance")

    val pipeline = new Pipeline().setStages(
      Array(ageIndexer, locationIndexer, publisherIndexer, yearOfPublicationIndexer,assembler, randomForestRegressor))

    val model = pipeline.fit(trainingData)
    model.save(args(1)+"_Model")

    println("Done Training")
    // Make predictions.
    val predictions = model.transform(testData)
//
    // Select example rows to display.
    predictions.select("Book-Title", "Book-Author","Predicted Book-Rating",  "indexedFeatures").show(5)
//    // Select example rows to display.
//    predictions.show()

    predictions.select("Book-Title", "Book-Author","Predicted Book-Rating",  "indexedFeatures").rdd.map(_.toString()).saveAsTextFile(args(1) + "_predictions")

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("Book-Rating")
      .setPredictionCol("Predicted Book-Rating")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    logger.info("Root Mean Squared Error (RMSE) on test data =" + rmse)
    val rmseoutput = sc.parallelize(Seq(("Root Mean Squared Error (RMSE) on test data =", rmse)))

    rmseoutput.saveAsTextFile(args(1)+ "_RMSE")
  }
}