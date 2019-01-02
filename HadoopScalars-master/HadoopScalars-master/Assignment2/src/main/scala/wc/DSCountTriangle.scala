package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object DSCountTriangle {
  case class follower(from: String, to: String)
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterFollowerCount <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("DataSetCountTriangle")
    val sc = new SparkContext(conf)
    var limit = 22000
    val spark: SparkSession = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val textFile = sc.textFile(args(0))
      .map(line => follower(line.split(",")(0), line.split(",")(1)))
      .filter(nodesTuple => nodesTuple.from.toLong <= limit && nodesTuple.to.toLong <= limit)
    // set edge dataset with from and to followers.
    val edgeDS = spark.createDataset(textFile)

    // calculating two paths from comparing from and to from the same table and joining on same table but on different columns
    val twoPaths = edgeDS.as("S1")
      .join(edgeDS.as("S2"))
      .where($"S1.to" === $"S2.from")
      .select($"S1.from".as("from"), $"S1.to".as("mid"), $"S2.from".as("midDup"), $"S2.to".as("to"))

    // selecting from and to paths from tables that are not equal and projecting the middle columns
    val twoPathsDS = twoPaths.toDF("from", "mid", "midDup", "to")
      .filter($"from" =!= $"to")
      .select("from", "to")

    // close triangle or get the triangles from join on originial
    val triangles = twoPathsDS.join(edgeDS, twoPathsDS("from") === edgeDS("to") && twoPathsDS("to") === edgeDS("from"))

    // since one triangle repeated 3 times we will divide by 3 to get the original count
    val finalTriangleCount = triangles.count()/3

    val countTriangle = sc.parallelize(Seq(("Number of Triangles in Twitter Dataset", finalTriangleCount)))

    // save number of triangle to file
    countTriangle.saveAsTextFile(args(1))
  }
}
