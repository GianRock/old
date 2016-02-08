package com.rock.twitterEventDetector.dbscanScala

import org.apache.spark.SparkContext
import org.apache.spark.mllib.util.Saveable

/**
  * Created by rocco on 23/01/2016.
  */
class DbscanModel extends Serializable with Saveable {
   override def save(sc: SparkContext, path: String): Unit = ???

  override protected def formatVersion: String = ???
}
