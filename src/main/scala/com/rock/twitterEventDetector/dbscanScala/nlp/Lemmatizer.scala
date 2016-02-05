package com.rock.twitterEventDetector.dbscanScala

import java.util
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreAnnotations._
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.util.CoreMap
import scala.collection.JavaConversions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by rocco on 28/01/2016.
  */
package object nlp {

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }
  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }
  def plainTextToLemmas(text: String,
                        pipeline: StanfordCoreNLP)  = {
    val document = new Annotation(text)
    pipeline.annotate(document);
    val lemmas: ArrayBuffer[String] = new ArrayBuffer[String]()
    val sentences: util.List[CoreMap] = document.get(classOf[CoreAnnotations.SentencesAnnotation])

    for (sentence <- sentences) {
      for (token <- sentence.get(classOf[CoreAnnotations.TokensAnnotation])) {
        val lemma =
          token.get(classOf[LemmaAnnotation])
        if(lemma.length>2 && isOnlyLetters(lemma)){
          lemmas+=lemma.toLowerCase()
        }
      }
    }


    lemmas
  }
  /*
  val stopWords = sc.broadcast(
    scala.io.Source.fromFile("stopwords.txt).getLines().toSet).value
  val lemmatized: RDD[Seq[String]] = plainText.mapPartitions(it => {
    val pipeline = createNLPPipeline()
    it.map { case(title, contents) =>
      plainTextToLemmas(contents, stopWords, pipeline)
    }
  }) */


}
