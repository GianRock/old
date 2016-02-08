package com.rock.twitterEventDetector.model

import java.io.Serializable
import java.util.Date

 import com.mongodb.BasicDBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.rock.twitterEventDetector.model.Model.AnnotationType.AnnotationType
import org.apache.lucene.search.similarities.Similarity
import org.apache.spark.mllib.linalg.SparseVector

/**
  * Created by rocco on 01/02/2016.
  */
object Model {

  object AnnotationType extends Enumeration {
    type AnnotationType = Value
    val Where,Who,What,When = Value
  }
  /**
    *
    * @param id
    * @param text
    * @param createdAt
    * @param hashTags
    * @param splittedHashTags
    */
  case class Tweet(id:Long,text:String,createdAt:Date,hashTags:List[String],splittedHashTags:Option[String]){
    def timeSimilarity(that:Tweet):Double={

      0d

    }
  }

  /**
    *
    * @param text the text of the hashtags
    * @param indices offsets (start,end) of the hashtag refferring to the text of the tweet
    */
  case class HashTag(text:String,indices:Tuple2[Int,Int])
  case class AnnotatedTweet(val tweet:Tweet,val tfIdfVector:SparseVector,val urisDbpedia:List[String])
  case class AnnotatedTweetWithDbpediaResources(val tweet:Tweet,val tfIdfVector:SparseVector, val dbpediaResoruceSet:Set[DbpediaResource])

  /**
    *
    * @param uriDBpedia
    * @param inLinks
    */
  case class DbpediaResource(val uriDBpedia:String,val inLinks:Set[String]) extends Similarity[DbpediaResource]{
    override def calculateSimilarity(that:DbpediaResource)=1.0
  }



  case class DbpediaAnnotation(val surfaceText:String, val start:Int, val kindOf:AnnotationType, val uriDBpedia:String){
    def this(bsonDoc:BasicDBObject)=this(bsonDoc.getString("surfaceText"),bsonDoc.getInt("start"),AnnotationType.withName(bsonDoc.getString("kindOf")),bsonDoc.getString("uriDBpedia"))
    def toMaps =dbpediaAnnotationToMap(this)
  }




  def dbpediaAnnotationToMap(dbpediaAnnotation: DbpediaAnnotation) = {

    dbpediaAnnotation match {
      case DbpediaAnnotation(surfaceText, start, kindOf, uriDBpedia)
      =>MongoDBObject("surfaceText"->surfaceText,"start"->start,"kindOf"->kindOf.toString,"uriDBpedia"->uriDBpedia)
     // case _=>  RuntimeException()
    }



   }


}
