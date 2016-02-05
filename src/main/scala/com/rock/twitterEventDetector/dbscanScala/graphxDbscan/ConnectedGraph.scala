package com.rock.twitterEventDetector.dbscanScala.graphxDbscan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, GraphLoader, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by rocco on 26/01/2016.
  */
object ConnectedGraph {

  def main(args: Array[String]) {
    val datasetC: String = "./resource/AggregationDatasetstep1-8.txt"
    val conf: SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[16]").set("spark.executor.memory", "1g")
    val sc: SparkContext = new SparkContext(conf)

      val graph: Graph[Int, Int] = GraphLoader.edgeListFile(sc, "data/notRepeatedNeighbors.txt",canonicalOrientation = true)

    // Find the connected components
     val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices
    // Join the connected components with the usernames
    val lines = sc.textFile("data/AggregationDatasetstep1-8.txt")
    val coordinates: RDD[(Long, String)] =lines.map{
      line=>
        val fields: Array[String] = line.split("\t")
        (fields(0).toLong, fields(1)+","+fields(2))
    }
    val ccByUsername = coordinates.join(cc).map {
      case (id, (username, cc)) => (username+","+ cc)
    }
    // Print the result
     println(ccByUsername.collect().mkString("\n"))
   }
}
