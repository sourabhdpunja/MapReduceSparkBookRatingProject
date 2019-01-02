package triangle

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.spark.sql
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object TriangleCountDS {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TwitterMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Twitter Follower")
    val sc = new SparkContext(conf)

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("AppName")
        .config("spark.master", "local")
        .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val textFile = sc.textFile(args(0))
    val edges = textFile.map(line => (line.split(",")(0), line.split(",")(1)))

    // Make Dataframe of edges
    val edgeDS = edges.toDF("from", "to");

    // Filter edges with id > 25000.
    val maxFilteredEdges = edgeDS
      .filter($"from" <= 22000 and $"to" <= 22000);

    // calculate path2 lengths
    // Broadcast filtered Edges
    val path2 = maxFilteredEdges.as("edgetable1").join(broadcast(maxFilteredEdges).as("edgetable2")).where($"edgetable1.to" === $"edgetable2.from").select("edgetable1.from", "edgetable2.to")


    // Calculate triangles from path2 and edges.
    val triangles = path2.as("path2")
      .join(broadcast(maxFilteredEdges).as("edgeTable1"),
        $"path2.to" === $"edgeTable1.from" && $"edgeTable1.to" === $"path2.from")

    // Triangle count divide by 3 for correct triangle count.
    val triangleCount = triangles.count() / 3.0

    val countTriangle = sc.parallelize(Seq(("Number of Triangles in Twitter Dataset", triangleCount)))

    // save number of triangle to file
    countTriangle.saveAsTextFile(args(1))
  }
}