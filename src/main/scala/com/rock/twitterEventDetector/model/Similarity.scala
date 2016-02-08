package com.rock.twitterEventDetector.model

/**
  * Created by rocco on 08/02/16.
  */
trait Similarity[T] {
  /**
    * Calcola la similarità fra questo oggetto,e quello specificato in input
    *
    * @param that
    * @return un double che indica la  distanza fra i due oggetti

    */
  def calculateSimilarity(that: T): Double
}