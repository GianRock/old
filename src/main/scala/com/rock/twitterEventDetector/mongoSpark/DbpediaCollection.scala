package com.rock.twitterEventDetector.mongoSpark

import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.MongoDBObject

import scala.collection.JavaConverters._

/**
  * Created by rocco on 03/02/2016.
  */
object DbpediaCollection {


  def findDbpediaResourceByURI(uriDBpedia:String):Option[DbpediaResource]={
    val res =MongoClient("localhost", 27017)("dbpedia").getCollection("pagelinks").findOne(MongoDBObject ("_id"->uriDBpedia))
    if(res!=null){
      val links=res.get("value").asInstanceOf[java.util.List[String]].asScala.toSet



      Some(new DbpediaResource(uriDBpedia,links))

    }
    else None

  }

  def main(args: Array[String]) {
    val c=findDbpediaResourceByURI("http://dbpedia.org/resource/Rome")
    println(c)
  }


}
