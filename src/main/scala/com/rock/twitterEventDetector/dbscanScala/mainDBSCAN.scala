package com.rock.twitterEventDetector.dbscanScala

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.graphx.{VertexId, VertexRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by rocco on 23/01/2016.
  */
object mainDBSCAN {

  def main(args: Array[String]) {
    val logFile: String = "./resource/AggregationDatasetstep1-8.txt"

    val conf: SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[16]").set("spark.executor.memory", "1g")
    //SparkConf conf = new SparkConf().setAppName("Simple Application");
    val sc: JavaSparkContext = new JavaSparkContext(conf)
    val lines: RDD[String] = sc.textFile(logFile)
    //  JavaRDD<Integer> lineLengths = lines.map(s ->

    println(lines.count())
    val coordinateRDD: RDD[(Long, CoordinateInstance)] =lines.map(
      f = line => {
        val parts: Array[String] = line.split("\t")
        val id: Long = parts(0).toLong
        val c: CoordinateInstance = new CoordinateInstance(id.asInstanceOf[java.lang.Long],parts(1).toDouble, parts(2).toDouble)

        (id, c)

      }
    )


    val dbscan=new DbscanScalaSparkWithGraphX(coordinateRDD,"sparkCordPar2",4,  1.16726175299287)
    val connectedComponents: VertexRDD[VertexId] =dbscan.run(sc);
    connectedComponents.collect().foreach(
      x=>println(x._1+" idcluster "+x._2)
    )
   val clusteredCoordinates= coordinateRDD.leftOuterJoin(connectedComponents).map {
      case (id, (coordinate, Some(cluster))) => (coordinate.x+","+coordinate.y+","+ cluster)
       case (id, (coordinate, None)) => (coordinate.x+","+coordinate.y+","+"-2")
    }
    /**
    val clusterdCoordinates = coordinateRDD.join(connectedComponents).map {
      case (id, (coordinate, cc)) => (coordinate.getX+","+coordinate.getY+","+ cc)
    }
*/
      //println(clusterdCoordinates.collect())
    // Print the result
  println(clusteredCoordinates.collect().mkString("\n"))


    //val clusters=connectedComponents.map(x=>x._2).distinct()
    //clusters.collect().foreach(cluster=>println(cluster))

    //connectedComponents.gr
  }

}
