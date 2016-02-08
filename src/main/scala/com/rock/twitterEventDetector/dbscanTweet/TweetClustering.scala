/*
package com.rock.twitterEventDetector.dbscanTweet

import java.{util, lang}
import java.util.Date
import edu.berkeley.cs.amplab.spark.indexedrdd.{IndexedRDD, KeySerializer}

import com.mongodb.casbah.commons.Imports
import com.mongodb. hadoop.{MongoOutputFormat, BSONFileOutputFormat}
import com.rock.twitterEventDetector.lsh.LSHModel
import com.rock.twitterEventDetector.model.Model.{AnnotatedTweetWithDbpediaResources, AnnotatedTweet, Tweet}
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import com.rock.twitterEventDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}

import com.twitter.chill.Tuple2Serializer
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import com.rock.twitterEventDetector.dbscanTweet.Distances._

import org.apache.hadoop.conf.Configuration
import org.apache.spark.graphx.{Graph, VertexId, VertexRDD}
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{FutureAction, SparkConf, SparkContext}
import org.bson.BSONObject

import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}
import scala.collection.parallel.ParSeq



case class MyTuple2(val band:Int,val signature:String)
/**
  * Created by rocco on 26/01/2016.
  */
object TweetClustering {


  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  def generateTfIdfVectors(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[(Long, SparseVector)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Vector)] = tweets mapPartitions  {
      it => {
        val analyzer = new MyAnalyzer()
        it.flatMap { case (idtweet, tweet) =>
          val tokenList = AnalyzerUtils.tokenizeText(analyzer, tweet.text).asScala

          if (tokenList.size >= 2) {
            Some(idtweet, hashingTF.transform(tokenList))

          }
          else None


        }


      }


    }

    tfVectors.cache()
    val idf = new IDF(2).fit(tfVectors.values)

    val norm: Normalizer = new Normalizer()
    val tfidf: RDD[(Long, SparseVector)] = tfVectors.map {
      tuple => {

        (tuple._1, idf.transform(tuple._2).toSparse)
      }
    }
    tfidf
  }
  /**
    * Generate tf-idf vectors from the a rdd containing tweets
    *
    * @param tweets
    * @param sizeDictionary
    * @return
    */
  def nlpPipeLine(tweets: RDD[(Long, Tweet)], sizeDictionary: Int): RDD[(Long, AnnotatedTweet)] = {

    val hashingTF = new HashingTF(sizeDictionary)
    // Load documents (one per line).
    val tfVectors: RDD[(VertexId, Tweet,Vector,List[String])] = tweets mapPartitions {
      it => {
        val analyzer = new MyAnalyzer()
        val dbpediaSpootligth=new DbpediaSpootLightAnnotator
        it.flatMap { case (idtweet, tweet) =>

          /**
            * per far si che gli hashtag abbiano un boost pari a 2.0
            * è sufficiente appendere a fine del testo del tweet tutti gli hashtag
            * in questo modo avranno tf pari a 2.
            */
          val textToTokenize:String =tweet.text+" "+tweet.splittedHashTags.getOrElse("")+" "+tweet.hashTags.mkString(" ");

          val tokenList = AnalyzerUtils.tokenizeText(analyzer, textToTokenize).asScala

          if (tokenList.size >= 2) {
            val urisDbpedia: List[String] =dbpediaSpootligth.annotateTextWithDbpediaUris(tweet.text).getOrElse(List.empty[String])
            Some(idtweet, tweet, hashingTF.transform(tokenList), urisDbpedia)
          }
          else None

        }
      }
    }

    tfVectors.cache()

    val idf = new IDF(2).fit(tfVectors.map(x=>x._3))
    val annotatedTweet = tfVectors.map (tuple =>
      (tuple._1,  new AnnotatedTweet(tuple._2 ,idf.transform(tuple._3).toSparse,tuple._4)))
    annotatedTweet
  }





  /**
    *  this function retrives  all the candidate neighbors of the document from the lsh model,
    *  with the id supplied.
    *
    *  all the id of documents who lie in the same bucket
    *
    * @param invertedLSH
    * @param indexedLSH
    * @param idQuery
    * @return
    */
  def getCandidateListFromIndexedRDD
  (invertedLSH:IndexedRDD[Long, Iterable[(Int, String)]],indexedLSH:IndexedRDD[String, Iterable[VertexId]],idQuery:Long ):List[Long]={
    val signatures=invertedLSH.get(idQuery).get
    val candidateList: List[Long] =signatures.flatMap {
      case (banda, signature) =>indexedLSH.get(banda+"-"+signature).filterNot(_==idQuery).get.toList
    }.toList
    candidateList
  }
  /**
    *
    * @param sc
    * @param data
    * @param minPts
    * @param eps
    */
  def startClusteringTweets(sc: SparkContext, data: RDD[(Long, Tweet)], lshModel: LSHModel,minPts: Int, eps: Double)  = {

    data.cache()
    /**
      *a partire dal modello lsh
      * creo un indexed rdd che ad ogni id documento
      * associa un interable di coppie (banda,signature)
      */
    val invertedLsh: RDD[(Long, Iterable[(Int, String)])] =lshModel.hashTables.map{
      case(hashkey,id)=>(id,hashkey)
    }.groupByKey()

    val indexedInvertedLsh: IndexedRDD[Long, Iterable[(Int, String)]] = IndexedRDD(invertedLsh).cache()


    /**
      * creo un indexRDD
      * che per ogni ad ogni (banda,signature) associa
      * la lista degli id dei documenti che ricadono
      * in tale bucket
      */
    val indexedHashTable: IndexedRDD[String, Iterable[VertexId]] =IndexedRDD(lshModel.hashTables.groupByKey()
      .map(x=>(x._1._1.toString+"-"+x._1._2,x._2))).cache()





    /**
      * primo passo da fare è generare
      * per ogni tweet vettori di hashingTF
      */
    val sizeDictionary = Math.pow(2, 18).toInt


    val tfIdfVectors: RDD[(Long, SparseVector)] = generateTfIdfVectors(data, sizeDictionary)
    val annotatedTweets=nlpPipeLine(data,sizeDictionary)
    tfIdfVectors.cache()
    //val tfIdfVectorsMap: Seq[(VertexId, SparseVector)] = tfIdfVectors.collect().toSeq
    // val lsh = new LSH(tfIdfVectors,sizeDictionary, numHashFunc =10, numHashTables = 13)


    /*

        val objectNeighborsList:ParSeq[(VertexId, Int)] = tfIdfVectorsMap.par.map {
          case (idTweetA: Long, tfidfVectorA: SparseVector) =>

            /**
              * retrive the candidate list-neighbors from the lsh model
              */

            val candidateNeighbors: mutable.Buffer[VertexId] =
              LshTableCollections.getCandidateList(idTweetA).asScala.map(Long2long)
              //lshModel.getCandidates(idTweetA).collect().toList

             (idTweetA,candidateNeighbors.size)


        }*/
    //objectNeighborsList.toList.foreach(x=>println(x._1+"  candidate List neighnbor "+x._2))

    val annotatedtweetsCollected: Seq[(Long, AnnotatedTweet)] =annotatedTweets.collect().toSeq

    /*
    val annotatedWithResources=annotatedtweetsCollected.map{
      annotatetTweet=> {
        val dbpediaResources =annotatetTweet._2.urisDbpedia.map {
          uriDbpedia => DbpediaResourceMongo.getDbediaResource(uriDbpedia)
        }.toSet
        (annotatetTweet._1,new  AnnotatedTweetWithDbpediaResources(annotatetTweet._2.tweet,annotatetTweet._2.tfIdfVector,dbpediaResources))
      }
    }.toSeq*/



    val objectNeighborsList: Seq[(VertexId, VertexId)] = annotatedWithResources.flatMap {
      case (idTweetA: Long, annotatedTweetA:AnnotatedTweetWithDbpediaResources) =>

        /**
          * retrive the candidate list-neighbors from the lsh model
          */
        val candidateNeighbors: List[Long] =getCandidateListFromIndexedRDD(indexedInvertedLsh,indexedHashTable,idTweetA)

        //lshModel.getCandidates(idTweetA).collect().toList


        val candidateVectors = annotatedWithResources.filter(x => candidateNeighbors.contains(x._1))

        /**
          * warn!! the candidate list should be filtered to avoid false positive.
          * i.e those
          * object who lies in the same bucket of current object,
          * but whose distance is greater than the given
          * treshold eps
          */
        val neighborList = candidateVectors flatMap {
          case (idTweetB, annotatedTweetB) =>
            val timeSimilarity=timeDecayFunction(annotatedTweetA.tweet.createdAt,annotatedTweetB.tweet.createdAt)

            if(timeSimilarity>=0){
              val cosSim =  cosineSimilarity(annotatedTweetA.tfIdfVector, annotatedTweetB.tfIdfVector)
              if((1-cosSim)<=2*eps){
                val semanticSim=Distances.semanticSimilarity(annotatedTweetA.dbpediaResoruceSet,annotatedTweetB.dbpediaResoruceSet)

                val similarity=timeSimilarity*((cosSim+semanticSim)/2)
                val distance=1d-similarity

                if (distance <= eps)
                  Some(idTweetA, idTweetB)
                else
                  None
              }else None
            } else None
        }
        neighborList


    }
    /*
    val objectNeighborsListOld: Seq[(VertexId, VertexId)] = tfIdfVectorsMap.flatMap {
      case (idTweetA: Long, tfidfVectorA: SparseVector) =>

        /**
          * retrive the candidate list-neighbors from the lsh model
          */
        val candidateNeighbors: List[Long] =getCandidateListFromIndexedRDD(indexedInvertedLsh,indexedHashTable,idTweetA)

        //lshModel.getCandidates(idTweetA).collect().toList

        print(idTweetA+" "+candidateNeighbors.mkString("_"))
        val candidateVectors = tfIdfVectorsMap.filter(x => candidateNeighbors.contains(x._1))

        /**
          * warn!! the candidate list should be filtered to avoid false positive.
          * i.e those
          * object who lies in the same bucket of current object,
          * but whose distance is greater than the given
          * treshold eps
          */
        val neighborList = candidateVectors flatMap {
          case (idTweetB, tfIdfVectorB) =>
            val cosineDist = 1d - cosineSimilarity(tfidfVectorA, tfIdfVectorB)

            if (cosineDist <= eps) Some(idTweetA, idTweetB)
            else
              None

        }
        neighborList


    }*/
    // objectNeighborsList.filter(x=>x._2.size>=minPts)


    /*
     val objectNeighborsList: List[(Long, Long)] =tfIdfVectorsMap.toList flatMap   {
      case(idTweetA:Long,tfidfVectorA:SparseVector)=>

        /**
          * retrive the candidate list-neighbors from the lsh model
          */
        val candidateNeighbors: List[Long] =lshModel.getCandidates(idTweetA).collect().toList


        val candidateVectors = tfIdfVectorsMap.filter(x => candidateNeighbors.contains(x._1))

        /**
          * warn!! the candidate list should be filtered to avoid false positive. i.e those
          * object who lies in the same bucket of current object,
          * but whose distance is greater than the given
          * treshold
          */
        val neighborList =candidateVectors flatMap {
          case (idTweetB,tfIdfVectorB)=>
            val cosineDist=cosine(tfidfVectorA,tfIdfVectorB)

            if(cosineDist<=eps) Some(idTweetA,idTweetB)
            else
              None

        }
        neighborList







    }

*/


    /**
      * a partire dall
      */
    val filteredneighList: RDD[(VertexId, VertexId)] =  sc.parallelize(objectNeighborsList).groupByKey().
      filter(x => x._2.size > minPts).flatMap {
      case (idCore: Long, listNeighbor: Iterable[Long]) => listNeighbor map {
        neighbor => (idCore, neighbor)

      }
    }.cache()

    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(filteredneighList, 1)
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices;


    // objectNeighborsList.foreach(x=>println(x._1+" vicinato "+x._2))
    connectedComponents

  }





  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setMaster("local[16]")
      .setAppName("annotations")
      .set("spark.executor.memory ", "10g")
    val sc = new SparkContext(sparkConf)






    val minDAte = TweetsCollection.findTweetById(255939718808760320L).getCreateAt
    println("min date value" + minDAte)

    val maxDate = new Date(minDAte.getTime + 10000)
    val tweetsfirst72H = SparkMongoIntegration.getTweetsFromDateOffset(sc, minDAte, 10)
    //  val tentweets=tweetsfirst72H.take(10);
    tweetsfirst72H.cache()
    println(" NUMBER Of tweeets" + tweetsfirst72H.count())

    println("finished loading tweets from mongo")
    /**
      * primo passo da fare è generare
      * per ogni tweet vettori di hashingTF
      */
    val sizeDictionary = Math.pow(2, 18).toInt
    //   val tfidfVectors = generateTfIdfVectors(tweetsfirst72H, sizeDictionary)

    //val lsh = new LSH(tfidfVectors, sizeDictionary, numHashFunc = 13, numHashTables = 10)
    //val model = lsh.run()
    //model.save(sc, "target/first72-13r10b")


    // val lshModelLoaded: LSHModel = LSHModel.load(sc, "target/first72-13r10b")

    println("finished loading lsh model")


    // startClusteringTweets(sc: SparkContext, tweetsfirst72H, lshModelLoaded,10,0.35)




    // val candidateList= lshModelLoaded.getCandidates(256561943450636288L).collect();
    //  println(candidateList)
    //val hashTables: RDD[((Int, String), VertexId)] =lshModelLoaded.hashTables
    //  val outputConfig: Configuration = new Configuration
    //outputConfig.set("mongo.output.uri", "mongodb://127.0.0.1:27017/" + Constants.MONGO_DB_NAME + "." + "lshTable")
    /*
          val lshTableBsons: RDD[(Null, BasicBSONObject)] =lshModelLoaded.hashTables.map{
            case((b:Int,signature:String),idtweet:Long)=> {

              var mapObj=Map("b"->b,"sig"->signature,"idtweet"->idtweet)
              var bson= new BasicBSONObject(mapObj.asJava)

              (null, bson)

            }

          }*/

    /**
      *lshTableBsons.saveAsNewAPIHadoopFile("file:///this-is-completely-unused",
      *classOf[Object],
      *classOf[BSONObject],
      *classOf[MongoOutputFormat[Object, BSONObject]],
      *outputConfig)
      *println("hash table dim"+lshModelLoaded.hashTables.count())
      *sc.stop()

      *val candidateList: RDD[VertexId] = lshModelLoaded.getCandidates(tentweets(0)._1)
      *println("candidate list count"+candidateList.count())
      *val candidateList2: RDD[VertexId] = lshModelLoaded.getCandidates(tentweets(1)._1)

      *println("candidate list coun2t"+candidateList2.count())


      */

    //val connectedGraphsCluster: VertexRDD[VertexId] =

    //

    //val clusters=connectedGraphsCluster.map(x=>x._2).distinct().collect()
    //  println(" NUMBER OF CLUSTER GENERATED "+clusters.size)


    val annotatedTweetRDD =tweetsfirst72H.mapPartitions {
      it => {
        val annotator=new DbpediaSpootLightAnnotator
        it.map {
          case (idtweet, tweet) => {
            val annotations=annotator.annotateText(tweet.text).getOrElse(List.empty[DbpediaAnnotation])

            (idtweet,annotations)
          }
        }
      }
    }
    annotatedTweetRDD.cache()
    //println(annotatedTweetRDD.count())
   // annotatedTweetRDD.foreachAsync(x=>print(x))

/*

      val outputConfig = new Configuration()
     outputConfig.set("mongo.output.uri",
       "mongodb://localhost:27017/"+Constants.MONGO_DB_NAME+".dbpediaAnnotations")




    annotatedTweetRDD.saveAsNewAPIHadoopFile(
      "file:///this-is-completely-unused",
      classOf[Object],
      classOf[BSONObject],
      classOf[MongoOutputFormat[Object, BSONObject]],
      outputConfig)
  }
}
*/
    val annotatedTweetsColl=annotatedTweetRDD.collect()
    annotatedTweetsColl.foreach(
      annotatedTweet=>DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(annotatedTweet._1,annotatedTweet._2)

      //AnnotationsCollection.insertDbpediaAnnotations(annotatedTweet._1.toLong,annotatedTweet._2.map(annotation=>annotation.toMaps).asJava)
        //DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(annotatedTweet._1,annotatedTweet._2)

    )


      /*
    val tweets1000: Array[(VertexId, Tweet)] =tweetsfirst72H.take(1000)
    val annotator=new DbpediaSpootLightAnnotator
    tweets1000.par.foreach{
      case(id,tweet)=>{
        val annotations: Option[List[DbpediaAnnotation]] =annotator.annotateText(tweet.text)
        DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(id,annotations.getOrElse(List.empty[DbpediaAnnotation]))

      }

    }*/

  }
}


*/
