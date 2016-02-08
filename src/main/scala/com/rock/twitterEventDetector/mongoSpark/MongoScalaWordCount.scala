package com.rock.twitterEventDetector.mongoSpark

import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
/*
   * JavaWordCount.java
   * Written in 2014 by Sampo Niskanen / Mobile Wellness Solutions MWS Ltd
   *
   * To the extent possible under law, the author(s) have dedicated all copyright and
   * related and neighboring rights to this software to the public domain worldwide.
   * This software is distributed without any warranty.
   *
   * See <http://creativecommons.org/publicdomain/zero/1.0/> for full details.
   */
import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.bson.BSONObject

/**
  * Created by rocco on 04/02/2016.
  */
object MongoScalaWordCount {
  def main(args: Array[String]) {


    val mongodbConfig: Configuration = new Configuration
    mongodbConfig.set("mongo.input.uri", "mongodb://127.0.0.1:27017/twitter.tweets")
    mongodbConfig.set("mongo.output.uri", "mongodb://127.0.0.1:27017/beowulf.output")
    val sparkConf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[*]")
      .set("spark.executor.memory", "8g");
    val sc = new SparkContext("local", "SparkExample", sparkConf)
    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri",
      "mongodb://localhost:27017/"+"twitter"+ "." +"tweets")
    val documents: RDD[(Object, BSONObject)] = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject]).filter(x=>x._2.containsField("cleaned_text"))        // Value type
    documents.cache()
    val words: RDD[String] = documents mapPartitions {
      it => {
        val analyzer = new MyAnalyzer()
        it.flatMap{ case (key, bsonDocument) =>

          /**
            * per far si che gli hashtag abbiano un boost pari a 2.0
            * è sufficiente appendere a fine del testo del tweet tutti gli hashtag
            * in questo modo avranno tf pari a 2.
            */
          val textToTokenize:String =bsonDocument.get("cleaned_text").asInstanceOf[String]

          val tokenList = AnalyzerUtils.tokenizeText(analyzer, textToTokenize).asScala

          tokenList

        }
      }
    }

    val ones= words.map{
      case(word)=>(word,1)
    }
    val counts=ones.reduceByKey(_+_)
    val save=counts.map{case(word,count)=>(word,MongoDBObject("count"->count))}
    save.collect().foreach(println)


    val outputConfig: Configuration = new Configuration
    outputConfig.set("mongo.output.uri", "mongodb://127.0.0.1:27017/beowulf.sparknuobo")





    // Save this RDD as a Hadoop "file".
    // The path argument is unused; all documents will go to "mongo.output.uri".
    save.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig)
    // save.saveAsNewAPIHadoopFile("file:///this-is-completely-unused", classOf[AnyRef], classOf[BSONObject], classOf[MongoOutputFormat[_, _]], outputConfig)
    sc.stop()


  }
}
