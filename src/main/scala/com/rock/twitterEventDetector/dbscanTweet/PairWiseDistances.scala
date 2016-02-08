package com.rock.twitterEventDetector.dbscanTweet

import com.rock.twitterFlashMobDetector.dbscanClustering.distance.Similarity
import scala.collection.JavaConversions._

/**
  * Created by rocco on 02/02/2016.
  */
object PairWiseDistances {
  private  def pairWiseSetSimdiffSize[T <: Similarity[T]](firstSet:  Set[T], secondSet: Set[T]): Double = {
    var sim: Double = 0.0
    for (elementFirstSet <- firstSet) {
      sim += calculateMaxSim(elementFirstSet, secondSet)
    }
    return sim / firstSet.size
  }

  /**
    * dati due insieme di elementi calcola una similarità  data la media delle
    * similarità delle coppie di elemnti con similarità massima.
    *
    * @param firstSet
    * @param secondSet
    * @return
    */
  def pairWiseSetSim[T <: Similarity[T]](firstSet: Set[T], secondSet: Set[T]): Double = {
    val n: Int = firstSet.size
    val m: Int = secondSet.size
    var sim: Double = 0.0
    if (n < m) {
      sim = pairWiseSetSimdiffSize(firstSet, secondSet)
    }
    else if (n == m) {
      sim = pairWiseSetSimEqualSize(firstSet, secondSet)
    }
    else {
      sim = pairWiseSetSimdiffSize(secondSet, firstSet)
    }
    return sim
  }

  /**
    * dati due insieme di elementi calcola una similarità  data la media delle
    * similarità delle coppie di elemnti con similarità massima.
    *
    * @param firstSet
    * @param secondSet
    * @return
    */
  private def pairWiseSetSimEqualSize[T <: Similarity[T]](firstSet: Set[T], secondSet: Set[T]): Double = {
    var sim: Double = 0.0
    var sim1: Double = 0.0
    import scala.collection.JavaConversions._
    for (elementFirstSet <- firstSet) {
      sim1 += calculateMaxSim(elementFirstSet, secondSet)
    }
    sim1 = sim1 / firstSet.size.toDouble
    var sim2: Double = 0.0
    for (elementSecondSet <- secondSet) {
      sim2 += calculateMaxSim(elementSecondSet, firstSet)
    }
    sim2 = sim2 / secondSet.size.toDouble
    sim = (sim1 + sim2) / 2.0
    return sim
  }

  /**
    * dati due insieme di elementi calcola una similarit� data la media delle
    * similarit� delle coppie di elemnti con similarit� massima.
    *
    * @param firstSet
    * @param secondSet
    * @return
    */
  def maxpairWiseSetSim[T <: Similarity[T]](firstSet: Set[T], secondSet:Set[T]): Double = {
    var sim: Double = 0.0
    var maxSim: Double = sim
    import scala.collection.JavaConversions._
    for (elementFirstSet <- firstSet) {
      sim = calculateMaxSim(elementFirstSet, secondSet)
      if (sim > maxSim) maxSim = sim
    }
    import scala.collection.JavaConversions._
    for (elementSecondSet <- secondSet) {
      sim = calculateMaxSim(elementSecondSet, firstSet)
      if (sim > maxSim) maxSim = sim
    }
    return maxSim
  }

  /**
    * Dato un elemento e un isnieme restituisce la similarit� massima
    *
    * @return similarità max tra l'elmento in input e
    */
  private def calculateMaxSim[T <: Similarity[T]](element: T, set: Set[T]): Double = {
    var maxSim: Double = 0.0
    for (elementSet <- set) {
      val sim: Double = element.calculateSimilarity(elementSet)
      if (sim > maxSim) {
        maxSim = sim
        if (maxSim == 1.0) return maxSim
      }
    }
    return maxSim
  }
}
