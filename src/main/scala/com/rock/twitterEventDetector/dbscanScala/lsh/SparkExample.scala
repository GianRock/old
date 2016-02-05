
package com.rock.twitterEventDetector.dbscanScala.lsh

import java.util.{Calendar, Date, GregorianCalendar}

import com.mongodb.hadoop.MongoInputFormat
import com.rock.twitterFlashMobDetector.model.twitter.MyTweet
import com.rock.twitterFlashMobDetector.utility.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, Document}

import scala.collection.JavaConverters._
/**
  * Created by rocco on 22/01/2016.
  */
object SparkExample extends App{
  val c1: Calendar = new GregorianCalendar
  val c2: Calendar = new GregorianCalendar
  c1.set(Calendar.YEAR, 2012)
  c2.set(Calendar.YEAR, 2012)

  c1.set(Calendar.MONTH, Calendar.OCTOBER)
  c2.set(Calendar.MONTH, Calendar.OCTOBER)
  c1.set(Calendar.DAY_OF_MONTH, 12)
  c2.set(Calendar.DAY_OF_MONTH, 14)
  c1.set(Calendar.HOUR_OF_DAY, 0)
  c2.set(Calendar.HOUR_OF_DAY, 0)
  c1.set(Calendar.MINUTE, 0)
  c1.set(Calendar.SECOND, 0)
  c2.set(Calendar.MINUTE, 0)
  c2.set(Calendar.SECOND, 0)
  c1.set(Calendar.MILLISECOND, 0)

  c2.set(Calendar.MILLISECOND, 0)
  val startTime: Date = c1.getTime
  val endTime: Date = c2.getTime

  val startStringDate="2012-10-10T01:00:01Z";
  val endStringDate="2012-10-11T01:00:01Z"
  val conditions:List[Document]=  List(new Document("created_at",new Document("$gte",startTime)) ,
    new Document("created_at",new Document("$lt", endTime)));
  val query: Document = new Document("$and", conditions.toList.asJava)
  System.out.println(query.toJson)
  val mongoConfig = new Configuration()
  // MongoInputFormat allows us to read from a live MongoDB instance.
  // We could also use BSONFileInputFormat to read BSON snapshots.
  // MongoDB connection string naming a collection to read.
  // If using BSON, use "mapred.input.dir" to configure the directory
  // where the BSON files are located instead.
  mongoConfig.set("mongo.input.uri",
    "mongodb://localhost:27017/"+ Constants.MONGO_DB_NAME+ "." + Constants.MONGO_TWEET_COLLECTION_NAME)
  mongoConfig.set("mongo.input.query", query.toJson)
  val sparkConf = new SparkConf()
  val sc = new SparkContext("local", "SparkExample", sparkConf)
   // Create an RDD backed by the MongoDB collection.
  val documents = sc.newAPIHadoopRDD(
    mongoConfig,                // Configuration
    classOf[MongoInputFormat],  // InputFormat
    classOf[Object],            // Key type
    classOf[BSONObject])        // Value type
  val tweets: RDD[(Long, MyTweet)] =documents.map{ case(id:Object,tweetBson:BSONObject)=>(id.asInstanceOf[Long],new MyTweet(tweetBson))}
  println(tweets.count())

}

