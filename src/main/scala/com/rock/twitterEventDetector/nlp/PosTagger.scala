package com.rock.twitterEventDetector.nlp

import java.io.StringReader

import edu.stanford.nlp.ling.TaggedWord
import edu.stanford.nlp.tagger.maxent.{MaxentTagger, TaggerConfig}

import scala.collection.JavaConversions._

/**
  * Created by rocco on 30/01/2016.
  */
class PosTagger {
  var MODEL_FILE: String = "./nlp/gate-EN-twitter.model"
  //"untokenizable=noneKeep,ptb3Escaping=false,normalizeFractions=false,normalizeSpace=true"
  val config: TaggerConfig = new TaggerConfig("-model", MODEL_FILE, "-untokenizable", "noneKeep", "-ptb3Escaping", "false")
  val tagger = new MaxentTagger(MODEL_FILE, config, false)

  /**
    * this function tags a sentence with pos tags
    * returning a map containg for each token the offsets  as key and the taggedToken as value
    * @param sentence
    * @return
    */
  def tagSentenenceToMap(sentence: String):Map[Int,TaggedWord]={
    val input: StringReader = new StringReader(sentence)
    val tokenized= MaxentTagger.tokenizeText(input).toList
    val taggedWords= tagger.process(tokenized).flatten.toList
    taggedWords.map {
      taggedWord =>(taggedWord.beginPosition,taggedWord)
    }.toMap
  }
}
