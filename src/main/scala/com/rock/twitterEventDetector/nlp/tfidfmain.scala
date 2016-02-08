package com.rock.twitterEventDetector.nlp

import java.io.InputStream
import java.util

import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import com.rock.twitterEventDetector.nlp.indexing.MyAnalyzer
import com.rock.twitterEventDetector.nlp.indexing.AnalyzerUtils
import scala.collection.JavaConverters._
import scala.collection.parallel.immutable.ParMap

/**
  * Created by rocco on 17/01/2016.
  */
object tfidfmain {

  def main(args: Array[String]) {
    //init spark context


    val numPartitions = 8
    val dataFile = "tweetsNov2.txt"
    val conf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //read data file in as a RDD, partition RDD across <partitions> cores
    val data = sc.textFile(dataFile, numPartitions)
  println(data.count())

    // Load documents (one per line).
    val documents: RDD[(Long,Seq[String])] = sc.textFile(dataFile).filter(line=>line.split("\t").size>1).mapPartitions(it=> {
      val stream : InputStream = getClass.getResourceAsStream("/nlp/oov.txt")
      val lines = scala.io.Source.fromInputStream( stream ).getLines.toList

      println("DIM FILE "+lines.length)
      val oovMap: ParMap[String, String] = lines.par.map{
        line=> {
          val parts= line.split("\t")
          (parts(0),parts(1))
        }
      }.toMap
      val analyzer=new MyAnalyzer()
      it.map(

        line => {
          val parts = line.split("\t")
          val x = parts.apply(0)

          val text=parts(1)
          val splits: Array[String] =text.split(" ")
          val normalizedText=splits.map{
            token=> oovMap.getOrElse(token,token)
          }.mkString(" ")

          if(normalizedText!=text){
            println(" HO NORMALIZZATO \n\r"+text +"\n\r"+normalizedText)
          }
          val se: util.List[String] = AnalyzerUtils.tokenizeText(analyzer,normalizedText);
          (parts.apply(0).toLong, se.asScala)
        })


    })
    documents.count()


  }
}