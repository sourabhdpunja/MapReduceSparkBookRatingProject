package triangle

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object TriangleCountRDD {
  
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

    val textFile = sc.textFile(args(0))
    val edges = textFile.map(line => (line.split(",")(0), line.split(",")(1)))
    val filteredEdges = edges.filter({case (k,v) => k.toLong <= 8000 && v.toLong <= 8000})

    // creating original edges from filtered data
    val reverseEdges = filteredEdges.map(line => (line._2, line._1))

    //Create Adjacency list
    var adjlistEdges = collection.mutable.Map[String, List[String]]()

    //Populate the adjlistEdges
    filteredEdges.collect().toList.map(edge => {
      val to = edge._1
      val from = edge._2
      var fromList = adjlistEdges.getOrElse(to,null)
      if(fromList == null){
        fromList = List(from)
      } else {
        fromList = fromList :+ from
      }
      adjlistEdges.put(to, fromList)
    })

    val broadcastAdjList = sc.broadcast(adjlistEdges)

    // Path2 Length calculated
    val len2Paths = filteredEdges.mapPartitions(iter => {
      iter.flatMap{
        case (from, to) =>
          val listOfEdges = broadcastAdjList.value.get(to).getOrElse(null)
          listOfEdges match {
            case null => Seq.empty
            case listOfEdges => listOfEdges.map(item => (from, item))
          }
      }
    }, preservesPartitioning = true)

    // Calculate Triangle Count
    val numberOfTriangles = len2Paths.join(reverseEdges).filter(item => item._2._1 == item._2._2).count()/3;

    logger.info("\tTriangle Count:" + numberOfTriangles)

    val countTriangle = sc.parallelize(Seq(("Number of Triangles in Twitter Dataset", numberOfTriangles)))
    countTriangle.saveAsTextFile(args(1))
  }
}