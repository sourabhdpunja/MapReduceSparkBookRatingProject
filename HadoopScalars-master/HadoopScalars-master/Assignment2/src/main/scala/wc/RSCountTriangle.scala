package wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object RSCountTriangle {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TriangleCountRDDRSMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Count the number of distinct triangles with RS Join on Twitter Dataset")/*.setMaster("local")*/
    val sc = new SparkContext(conf)

    // ================
    // Delete output directory, only to ease local development; will not work on AWS.
    // val hadoopConf = new org.apache.hadoop.conf.Configuration
    // val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    // try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    var limit = 22000

    // createing following edges with X following Y => (X,Y) i.e X is following Y
    var followerEdges = sc.textFile(args(0))
      .map(line => {
        val nodes = line.split(",")
        (nodes(1), nodes(0))
      }).filter(nodesTuple => nodesTuple._1.toLong <= limit && nodesTuple._2.toLong <= limit)

    // creating original edges from filtered data
    val realEdges = followerEdges.map(line => (line._2, line._1))

    // creating length 2 paths i,e, one hop paths
    val pathTwoLength = realEdges.join(followerEdges)
      .filter { case (y, (x, z)) => x != z }

    val path2LengthWithEndPoints = pathTwoLength.map { case (y, (x, z)) => (z, x) }

    // Finding triangle i.e if edge (X,Y), (Y,Z) and (Z,X)
    val twoHopTriangle = path2LengthWithEndPoints.join(followerEdges)
      .filter { case (y, (x, z)) => x == z }

    val triangleCounts = twoHopTriangle.count()
    // final triangle count will be triangle count divided by 3 since each triangle repeats 3 times.
    val finalTriangleCount = triangleCounts / 3

    logger.info("\tTriangle Count:" + finalTriangleCount)

    val countTriangle = sc.parallelize(Seq(("Number of Triangles in Twitter Dataset", finalTriangleCount)))
    countTriangle.saveAsTextFile(args(1))
  }
}
