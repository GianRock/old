package com.rock.twitterEventDetector.dbscanScala

/**
  * Created by rocco on 08/02/16.
  */
case class CoordinateInstance(x:Double,y:Double) {
  def distance(that:CoordinateInstance):Double =   Math.sqrt(Math.pow((this.x - that.x), 2) + Math.pow(this.y - that.y, 2))


}
