package com.rock.twitterEventDetector.dbscanScala.mongoSpark

import com.mongodb.{BasicDBList, DBObject}
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.{ValidBSONType, MongoDBObject}
import com.mongodb.casbah.commons.ValidBSONType.DBObject
import model.Model.DbpediaResource

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
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
