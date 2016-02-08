package com.rock.twitterEventDetector.nlp

import java.io.InputStream
import java.util
import javax.annotation.Resource

import com.rock.twitterEventDetector.dbscanScala.model.Model.DbpediaAnnotation
import com.rock.twitterEventDetector.dbscanScala.mongoSpark.DbpediaAnnotationCollection
import com.rock.twitterEventDetector.dbscanScala.nlp.PosTagger
import com.rock.twitterEventDetector.model.Model.DbpediaAnnotation
import edu.stanford.nlp.ling.TaggedWord
import model.Model.DbpediaAnnotation
import mongoSpark.DbpediaAnnotationCollection
import scala.io.Source._
import scala.collection.immutable.TreeMap
import scala.reflect.io.Path

/**
  * Created by rocco on 30/01/2016.
  */
object as {

    def main(args: Array[String]) {




      val stream : InputStream = getClass.getResourceAsStream("/nlp/oov.txt")
      val lines = scala.io.Source.fromInputStream( stream ).getLines.toList
/*
      println("DIM FILE "+lines.length)
      val oovMap = lines.par.map{
        line=> {
          val parts= line.split("\t")
          println(parts.size)
          (parts(0),parts(1))
        }
      }*/

      //val stanford= Lemmatizer.createNLPPipeline()
    //  val lemmas=  Lemmatizer.plainTextToLemmas("Awesome. RT  Geez.  tweets a picture of a $38,091 bill from rookie night. Hope Shea had some help picking up that one",stanford)
    //  println(lemmas.mkString(" - "))
      val g: String = "Looks like the cas flash mob MJ   dance will either on Saturday 27th or Sunday 28th- subject to weather, check your email for infoy"

      val posTagger=new PosTagger
      val posTagMap =posTagger.tagSentenenceToMap(g)
      posTagMap.foreach{
        tagged=>println(tagged)
      }


      val dbpediaAnn=new DbpediaSpootLightAnnotator()

       val c: List[DbpediaAnnotation]=dbpediaAnn.annotateText(g).get
      c.foreach(
        x=>println(x.toMaps)
      )
      DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(2433l,c)


    }

}
