package com.rock.twitterEventDetector.dbscanScala.nlp

import java.net.URLEncoder
import java.util

import com.rock.twitterFlashMobDetector.model.annotation.Annotation
import com.rock.twitterFlashMobDetector.nlp.annotator.rest.RestClient
import edu.stanford.nlp.ling.{HasWord, TaggedWord}
import model.Model.AnnotationType
import model.Model.AnnotationType._
import model.Model._
import org.json.{JSONArray, JSONObject}

import scala.collection.JavaConversions._
import scala.collection.immutable.IndexedSeq
import scala.util.{Try, Success, Failure}
import model._

/**
  * Created by rocco on 30/01/2016.
  */
class DbpediaSpootLightAnnotator extends  Serializable{
  val dbPediaRelvance=0.8
  val dbPediaPercentageOfSecondRank=0.5
  val posTagger=new PosTagger()
  val TYPE_WHERE=Set("Schema:Place")
  val TYPE_WHO=Set("")
  val TYPE_WHEN=Set("")

  val UNWANTED_DBPEDIA_URLS:Set[String]=Set(
    "http://dbpedia.org/resource/Google",
    "http://dbpedia.org/resource/Twitter",
    "http://dbpedia.org/resource/World",
    "http://dbpedia.org/resource/Twitter",
    "http://dbpedia.org/resource/Noon",
    "http://dbpedia.org/resource/Girl",
    "http://dbpedia.org/resource/Update_(SQL)",
    "http://dbpedia.org/resource/Video",
    "http://dbpedia.org/resource/Music",
    "http://dbpedia.org/resource/Adobe_Premiere_Pro"
  )
  val STOP_SURFACES:Set[String]=Set("SURPRISE","YOUTUBE","VIMEO","TWITTER","DAY","THURSDAY","SURPRISE", "OURSELVES")

  /**
    * url Del server su cui è installato dbpedia spootlight
    */
  val URL_DB_PEDIA: String = "http://localhost:2222/rest/annotate"
  /**
    *
    */
  private val serialVersionUID: Long = -5269556951100349492L
  /**
    * importanza minima della risorsa dbpedia, risorse aventi supporto al di
    * sotto di tale supporto soglia verrano scartate
    */
  var SUPPORT: Int = 40
  //private transient  GenericObjectPool<MyPosTagger> posTaggerPool;
  //private transient MyPosTagger posTagger  ;
  /**
    * The Confidence configuration applies two checks: 1. The similarity score
    * of the first ranked entity must be bigger than a threshold. 2. The gap
    * between the similarity score of the first and second ranked entity must
    * be bigger than a relative threshold.
    */
  var CONFIDENCE: Double = 0.25
  /**
    * intero che indica la prioritÃ  dell'annotazione. Tale priorità è utile
    * nel caso in cui vi sia più di un annotazione sulla medesima sottostringa
    * del testo. Varia tra 0 e n dove n è¨ il numero degli annotatori
    */


  /**
    * this method annotate the text of a given tweet with
    * the annotations returned by the DbpediaSpootligh Annotator.
    * Before submitting the text to the annotator, it first checks if
    * the the tweet is altready annotated.
    * @param tweet
    * @return
    */
  def annotateTweet(tweet:Tweet):List[DbpediaAnnotation]={
    ???
  }

  def decideTypeAnnotation(typesString:String):AnnotationType = {
    if(typesString!=null && typesString.length>0) {
      val types: Array[String]=typesString.split(",")

      if (types.contains("DBpedia:Place")) AnnotationType.Where
      else if (types.contains("DBpedia:Person") || types.contains("DBpedia:Organisation")) AnnotationType.Who
      else if (types.contains("DBpedia:Time")) AnnotationType.When
      else AnnotationType.What
    }else AnnotationType.What



  }
  def annotateText(text:String):Option[List[DbpediaAnnotation]]={
    val textEncoded: String = URLEncoder.encode(text, "UTF-8")



    val typesToAnnotate: String = "&types=DBpedia:Activity,DBpedia:Event,DBpedia:Organisation,DBpedia:Person,DBpedia:Place,DBpedia:Work,DBpedia:Agent,dbpedia-owl:Work"
    val url: String = URL_DB_PEDIA + "?text=" + textEncoded + "&confidence=" + CONFIDENCE + "&support=" + SUPPORT + "&spotter=Default" + typesToAnnotate

    val resp = Try(RestClient.doGet(url, true))
    //println(resp)
    resp match {
      case Failure(ex) =>
        println(ex)
        None
      case Success(value) =>
        Some(extractDbpediaAnnotationFromJsonResponse(text, value))
    }
  }


  /**
    *
    * @param text the text you want to annotate through DBpedia Spootlight
    * @return None if no annotation is returned by the web service , an option made of the list
    *         containing the annotation, otherwise
    */
  def annotateTextWithDbpediaUris(text: String): Option[List[String]] = {
    val textEncoded: String = URLEncoder.encode(text, "UTF-8")



    val typesToAnnotate: String = "&types=DBpedia:Activity,DBpedia:Event,DBpedia:Organisation,DBpedia:Person,DBpedia:Place,DBpedia:Work,DBpedia:Agent,dbpedia-owl:Work"
    val url: String = URL_DB_PEDIA + "?text=" + textEncoded + "&confidence=" + CONFIDENCE + "&support=" + SUPPORT + "&spotter=Default" + typesToAnnotate

    val resp = Try(RestClient.doGet(url, true))
    println(resp)
    resp match {
      case Failure(ex) =>
        println(ex)
        None
      case Success(value) =>
        Some(extractUris(text, value))
    }
  }


  /**
    * questo metodo stabilisce se una data sottostringa del testo @surface, sia composta o meno, da almeno
    * un nome proprio.
    *
    * @param surface
    * @param start
    * @param posTagMap
    * @param text
    * @return
    */
  def containsAtLeastOneNoun(surface: String, start: Int, posTagMap: Map[Int, TaggedWord], text: String): Boolean = {
    getTaggedWordsBtwOffsets(start, start + surface.length, posTagMap.toMap)
      .count(taggedword => taggedword.tag().startsWith("N"))>0
  }


  /**
    * retrives all the entries in the map posTagMap whose entries lies between the  Range start,end
    *
    * @param start
    * @param end
    * @param posTagMap
    * @return
    */
  private def getTaggedWordsBtwOffsets(start: Int, end: Int, posTagMap:Map[Int,TaggedWord]):List[TaggedWord] = Range(start, end+1).flatMap(offset => posTagMap.get(offset)).toList


  /**
    *
    * @param text
    * @param jsonAnnotations
    * @return
    */
  def extractDbpediaAnnotationFromJsonResponse(text: String, jsonAnnotations: String)={



    var resultJSON: JSONObject =  new JSONObject(jsonAnnotations)


    if (resultJSON.has("Resources")) {
      val posTagMap = posTagger.tagSentenenceToMap(text)
      val entities = resultJSON.getJSONArray("Resources")
      val relevanceTreshold = 0.8


      //   val tokens: List[HasWord] = MaxentTagger.tokenizeText(new StringReader(surface)).toList.flatten
      val urisDbpedia:IndexedSeq[DbpediaAnnotation]=for{
        i <-0 until entities.length()
        entity: JSONObject=entities.getJSONObject(i)
        surface: String = entity.getString("@surfaceForm")
        uriDbPedia: String = entity.getString("@URI")
        start: Integer = Integer.valueOf(entity.getString("@offset"))
        relvance: Double = entity.getDouble("@similarityScore")
        percentageOfSecondRank: Double = entity.getDouble("@percentageOfSecondRank")
        types: String = entity.getString("@types")
        dbpediaAnnotation=new DbpediaAnnotation(surface,start,decideTypeAnnotation(types),uriDbPedia)

        if(relvance> dbPediaRelvance && percentageOfSecondRank <= dbPediaPercentageOfSecondRank
          && containsAtLeastOneNoun(surface,start,posTagMap,text)
          && !UNWANTED_DBPEDIA_URLS.contains(uriDbPedia)
          && !STOP_SURFACES.contains(surface.toUpperCase))

      }yield dbpediaAnnotation

      urisDbpedia.toList
    }else{
      List.empty[DbpediaAnnotation]
    }


  }
  /**
    *
    * @param text
    * @param jsonAnnotations
    * @return
    */
  def extractUris(text: String, jsonAnnotations: String)={



    var resultJSON: JSONObject =  new JSONObject(jsonAnnotations)


    if (resultJSON.has("Resources")) {
      val posTagMap = posTagger.tagSentenenceToMap(text)
      val entities = resultJSON.getJSONArray("Resources")
      val relevanceTreshold = 0.8


      //   val tokens: List[HasWord] = MaxentTagger.tokenizeText(new StringReader(surface)).toList.flatten
      val urisDbpedia:IndexedSeq[String]=for{
        i <-0 until entities.length()
        entity: JSONObject=entities.getJSONObject(i)
        surface: String = entity.getString("@surfaceForm")
        uriDbPedia: String = entity.getString("@URI")
        start: Integer = Integer.valueOf(entity.getString("@offset"))
        relvance: Double = entity.getDouble("@similarityScore")
        percentageOfSecondRank: Double = entity.getDouble("@percentageOfSecondRank")


        if(relvance> dbPediaRelvance && percentageOfSecondRank <= dbPediaPercentageOfSecondRank
          && containsAtLeastOneNoun(surface,start,posTagMap,text)
          && !UNWANTED_DBPEDIA_URLS.contains(uriDbPedia)
          && !STOP_SURFACES.contains(surface.toUpperCase))

      }yield uriDbPedia

      urisDbpedia.toList
    }else{
      List.empty[String]
    }


  }



















}
