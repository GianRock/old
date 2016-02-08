package com.rock.twitterEventDetector.mongoSpark

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{BasicDBObject, DBObject, MongoException}

import scala.collection.JavaConverters._

/**
  * Created by rocco on 03/02/2016.
  */
object DbpediaAnnotationCollection {
  def inserDbpediaAnnotationsBulk(  annotations:Iterator[(Long, Option[List[DbpediaAnnotation]])])={


    val collection=MongoCLientSingleton.clientMongo(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
    val bulkWrites=collection.initializeUnorderedBulkOperation()
     annotations.foreach(annotation=>{
       bulkWrites.insert(MongoDBObject("_id"->annotation._1,"annotations"->annotation._2.get.map(ann=>ann.toMaps)))
    })

    println(" ADDING "+annotations.size+ " annotations to db")
    bulkWrites.execute()


  }
  def inserDbpediaAnnotations(  annotations:Iterator[(Long, Option[List[DbpediaAnnotation]])])={


    val collection=MongoCLientSingleton.clientMongo(MONGO_DB_NAME).getCollection("dbpediaAnnotations")
     annotations.foreach(annotation=>{
       try{
       collection.insert(MongoDBObject("_id"->annotation._1,"annotations"->annotation._2.get.map(ann=>ann.toMaps)))

       }catch {
         case foo: MongoException => println(foo)
       }
    })



  }

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
