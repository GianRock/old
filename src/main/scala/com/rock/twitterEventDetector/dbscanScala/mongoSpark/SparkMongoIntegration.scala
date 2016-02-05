package com.rock.twitterEventDetector.dbscanScala.mongoSpark

import java.util.{Calendar, Date, GregorianCalendar}

import com.mongodb.hadoop.MongoInputFormat
import com.rock.twitterFlashMobDetector.db.mongoDB.TweetsCollection
import com.rock.twitterFlashMobDetector.model.twitter.MyTweet
import com.rock.twitterFlashMobDetector.utility.Constants
import nlp.DbpediaSpootLightAnnotator
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.{BSONObject, Document}
import org.joda.time.DateTime
import model.Model._

import scala.collection.JavaConverters._
/**
  * Created by rocco on 23/01/2016.
  */
object SparkMongoIntegration {



  def HOUR_MILLISEC:Long=3600000L;
  /**
    *
    * @param sc
    * @param query
    * @return
    */
  def getTweetsAsRDD(sc:SparkContext,query:Document,  createSplits:Boolean=true):RDD[(Long,MyTweet)]={

    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri",
      "mongodb://localhost:27017/"+ Constants.MONGO_DB_NAME+ "." + Constants.MONGO_TWEET_COLLECTION_NAME)
    if(query!=null)
      mongoConfig.set("mongo.input.query", query.toJson)
    //  mongoConfig.set("mongo.input.key","created_at")
    mongoConfig.set("mongo.input.split.create_input_splits", "" + createSplits)
    mongoConfig.set("mongo.input.split.use_range_queries",""+true)
    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])        // Value type
    val tweets: RDD[(Long, MyTweet)] =documents.map{
        case(id:Object,tweetBson:BSONObject)=>(id.asInstanceOf[Long],new MyTweet(tweetBson))
      }
    tweets

  }
  /**
    *
    * @param sc
    * @param query
    * @return
    */
    def getTweetsAsTupleRDD(sc:SparkContext,query:Document,  createSplits:Boolean=true):RDD[(Long, Tweet)]={

    val mongoConfig = new Configuration()
    mongoConfig.set("mongo.input.uri",
      "mongodb://localhost:27017/"+ Constants.MONGO_DB_NAME+ "." + Constants.MONGO_TWEET_COLLECTION_NAME)
    if(query!=null)
      mongoConfig.set("mongo.input.query", query.toJson)
    mongoConfig.set("mongo.input.split.create_input_splits", "" + createSplits)
    mongoConfig.set("mongo.input.split.use_range_queries",""+true)
    val documents = sc.newAPIHadoopRDD(
      mongoConfig,                // Configuration
      classOf[MongoInputFormat],  // InputFormat
      classOf[Object],            // Key type
      classOf[BSONObject])        // Value type
    val tweets=documents.map{
        case(id:Object,tweetBson:BSONObject)=>{
          val hashtags =  tweetBson.get("entities").asInstanceOf[BSONObject].get("hashtags").asInstanceOf[java.util.List[BSONObject]].asScala.toList;
          val hashTagsValues=hashtags.map {
            hashtag => {
            //  val indices: List[Integer] = hashtag.get("indices").asInstanceOf[java.util.List[Integer]].asScala.toList
            //  HashTag(hashtag.get("text").asInstanceOf[String], (indices(0), indices(1)))
              hashtag.get("text").asInstanceOf[String]
            }
          }



          val splittedHashTags = if (tweetBson.containsField("splitted_hashtags")) Some(tweetBson.get("splitted_hashtags").asInstanceOf[String]) else None

          val tweet=new Tweet(id.asInstanceOf[Long],tweetBson.get("cleaned_text").toString,tweetBson.get("created_at").asInstanceOf[Date],hashTagsValues,splittedHashTags)
          (id.asInstanceOf[Long],tweet)
        }


      }



    tweets

  }
  /**
    * retrive all tweets in the time interval [dateStart,dateEnd[
    *
    * @param sc sparkContext
    * @param startDate date from which start search tweets (inclusive)
    * @param hourNumbers numberHours
    * @return an RDD consisting of tuple id,MyTweet of all tweets in the collection between those dates
    */
  def getTweetsFromDateOffset(sc:SparkContext,startDate: Date,hourNumbers:Int):RDD[(Long,Tweet)]={

    val endDate=new Date(startDate.getTime+HOUR_MILLISEC*hourNumbers)
    val conditions:List[Document]=  List(new Document("created_at",new Document("$gt",startDate)) ,
      new Document("created_at",new Document("$lt", endDate)));
    val query: Document = new Document("$and", conditions.toList.asJava)
    getTweetsAsTupleRDD(sc,query,false)

  }

  /**
    * retrive all tweets in the time interval [dateStart,dateEnd[
    *
    * @param sc sparkContext
    * @param startDate date from which start search tweets (inclusive)
    * @param endDate date value until search tweets (exclusive)
    * @return an RDD consisting of tuple id,MyTweet of all tweets in the collection between those dates
    */
  def getTweetsAsRDDInTimeInterval(sc:SparkContext,startDate: Date,endDate:Date):RDD[(Long,Tweet)]={
    val conditions:List[Document]=  List(new Document("created_at",new Document("$gte",startDate)) ,
      new Document("created_at",new Document("$lt", endDate)));
    val query: Document = new Document("$and", conditions.toList.asJava)
    getTweetsAsTupleRDD(sc,query)

  }


  def main(args: Array[String]) {
    val c1: Calendar = new GregorianCalendar
    val c2: Calendar = new GregorianCalendar
    c1.set(Calendar.YEAR, 2012)
    c2.set(Calendar.YEAR, 2012)

    c1.set(Calendar.MONTH, Calendar.OCTOBER)
    c2.set(Calendar.MONTH, Calendar.NOVEMBER)
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
    val conditions:List[Document]=  List(new Document("created_at",new Document("$gte",TweetsCollection.findMinDateValue())) ,
      new Document("created_at",new Document("$lt", endTime)));
    val query: Document = new Document("$and", conditions.toList.asJava)
     val sparkConf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[*]")
    val sc = new SparkContext("local", "SparkExample", sparkConf)

    val tweets: RDD[(Long, Tweet)] =getTweetsAsTupleRDD(sc,query,false)



  val firstTweets=tweets.take(100)
    val annotator=new DbpediaSpootLightAnnotator

    firstTweets.par.foreach{
      case(id,tweet)=>{
        val annotations: List[DbpediaAnnotation] =annotator.annotateTweet(tweet)
        DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(id,annotations)
      }


    }

     System.out.println(tweets.count())
    sc.stop()
  }
}
