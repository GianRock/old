package com.rock.twitterEventDetector.dbscanScala

/**
  * Created by rocco on 08/02/16.
  */
trait Distance[T] {
  /**
    * Calcola la similarità fra questo oggetto,e quello specificato in input
    *
    * @param that
    * @return un double che indica la  distanza fra i due oggetti

    */
  def distance(that: T): Double
}