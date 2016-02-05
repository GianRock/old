package com.rock.twitterEventDetector.dbscanScala.mongoSpark

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient

import scala.collection.mutable
import scala.language.reflectiveCalls

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

import com.mongodb.casbah.Imports._
import com.mongodb.util.JSON
/**
  * Created by rocco on 28/01/2016.
  */
object LshCollectionScala {
  // Use a Connection String


    def main(args: Array[String]) {
      val mongoClient: MongoClient = MongoClient("localhost", 27017)
      val db = mongoClient("tweetEventDataset")
      val collNames: mutable.Set[String] =db.collectionNames
      collNames.foreach(println)
  val collection: MongoCollection =db("tweets")
      val firstDoc=db.getCollection("tweets").findOne().toString
  println(firstDoc)

      val parallelScanOptions = ParallelScanOptions(2, 200)
      val cursors = collection.parallelScan(parallelScanOptions)


      for (cursor <- cursors) {
        while (cursor.hasNext) {
          println(cursor.next())
        }
      }

    }
  }
