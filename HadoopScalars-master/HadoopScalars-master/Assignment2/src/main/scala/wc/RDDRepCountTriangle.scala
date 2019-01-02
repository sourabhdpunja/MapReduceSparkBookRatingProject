package wc

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDRepCountTriangle {
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.TriangleCountRDDRSMain <input dir> <output dir>")
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("Count the number of distinct triangles with RS Join on Twitter Dataset").setMaster("local")
    val sc = new SparkContext(conf)

    // ================
    // Delete output directory, only to ease local development; will not work on AWS.
    // val hadoopConf = new org.apache.hadoop.conf.Configuration
    // val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    // try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    var limit = 22000

    // createing following edges with X following Y => (X,Y) i.e X is following Y
    var filteredEdges = sc.textFile(args(0))
      .map(line => {
        val nodes = line.split(",")
        (nodes(1), nodes(0))
      }).filter(nodesTuple => nodesTuple._1.toLong <= limit && nodesTuple._2.toLong <= limit)

    var adjlistEdges = collection.mutable.Map[String, List[String]]()
    var listFilteredEdges = filteredEdges
    listFilteredEdges.collect().toList.map(edge => {
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

    def parseTuple(tuple: (String, String)) : (Unit) = {
      System.out.println("**** "+tuple._1);
      System.out.println(tuple._2);
    }
    listFilteredEdges.foreach(println)

    //    val result = filteredEdges.mapPartitions(iter => iter.map(x => parseTuple(x)));
    println("********FINAL********")
    filteredEdges.mapPartitions(iter => iter.map(x => parseTuple(x)))
    print("****************")
  }
}
