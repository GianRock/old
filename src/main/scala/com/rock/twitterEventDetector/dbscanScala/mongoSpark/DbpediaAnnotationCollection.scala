package com.rock.twitterEventDetector.dbscanScala.mongoSpark

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{MongoException, BasicDBObject, DBObject}

import com.rock.twitterFlashMobDetector.utility.Constants._
import model.Model.DbpediaAnnotation
import scala.collection.JavaConverters._
import model._

/**
  * Created by rocco on 03/02/2016.
  */
object DbpediaAnnotationCollection {


    def insertDbpediaAnnotationsOfTweet(idTweet:Long,annotations:List[DbpediaAnnotation])={

      val collection=MongoCLientSingleton.clientMongo(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
      try{
        collection.insert(MongoDBObject("_id"->idTweet,"annotations"->annotations.map(annotation=>annotation.toMaps)))

      }catch {
        case foo: MongoException => println(foo)
      }


    }

  /**
    * retrive the annotations of tweet
    * it will reurn Some(of the list made of Dbpedia Annotations object]
    * None if the tweet isn't altready annotated through dbpedia Spootlight
 *
    * @param idTweet
    * @return
    */
    def getAnnotationsOfTweet(idTweet:Long):Option[List[DbpediaAnnotation]]={


    val collection=MongoCLientSingleton.clientMongo(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
      val result: DBObject =collection.find(MongoDBObject("_id"->idTweet)).one()
      if(result==null){
        None
      }else{
        val annotations=result.get("annotations").asInstanceOf[java.util.List[BasicDBObject]].asScala.map(bson=>new DbpediaAnnotation(bson)).toList
         annotations.foreach(println)
        Some(annotations)
      }
    }

    def main(args: Array[String]) {
      val annotations=DbpediaAnnotationCollection.getAnnotationsOfTweet(33l)
      println(annotations)

    }


}
