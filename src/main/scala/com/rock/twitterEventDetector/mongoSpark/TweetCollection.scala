package com.rock.twitterEventDetector.mongoSpark

import java.util.Date

import com.mongodb.{DBCursor, BasicDBObject, DBObject}
import com.mongodb.casbah._
import com.mongodb.casbah.commons.MongoDBObject
import com.rock.twitterEventDetector.configuration.Constant
import com.rock.twitterEventDetector.model.Model.{DbpediaAnnotation, DbpediaResource, Tweet}
import com.rock.twitterEventDetector.nlp.DbpediaSpootLightAnnotator
import  com.rock.twitterEventDetector.configuration.Constant._
import scala.annotation.tailrec
import scala.concurrent.Future

/**
  * Created by rocco on 07/02/16.
  */
object TweetCollection {
/*s
  def generateCouplesFromListTailRecursive(lista:List[Long]):Set[(Long,Long)]={

    def generate(list:List[Long],acc:Set[(Long,Long)])= {
      lista match {
        case Nil=>acc
        case head::h2::tail=>
      }

    }
    // require(lista.size>1)
    lista match{
      case Nil=>Nila
      //  case head::Nil=>Nil
      case head::head2::tail=>
        (head,head2)::generateCouplesFromList(head::tail):::generateCouplesFromList(head2::tail)
    }
  }*/



  def generaMio(lista:List[Int]):List[(Int,Int)]={
  // require(lista.size>1)
    lista match{
    case Nil=>Nil
    case head::Nil=>Nil
     case head::head2::tail=>
       (head,head2)::generaMio(head::tail):::generaMio(head2::tail)
   }
 }



 def findMinValueDate():Date={
   val doc =MongoClient("localhost", 27017)(Constant.MONGO_DB_NAME)
     .getCollection(Constant.MONGO_TWEET_COLLECTION_NAME).find(MongoDBObject(),MongoDBObject("_id"->0,"created_at"->1)).sort(MongoDBObject("created_at"->1)).limit(1).one()

  doc.get("created_at").asInstanceOf[Date]

 }

  /**
    *
    * @param idTweet
    * @return
    */
  def findTweetById(idTweet:Long):Option[Tweet]={
    val res: BasicDBObject =MongoClient("localhost", 27017)(Constant.MONGO_DB_NAME).getCollection(Constant.MONGO_TWEET_COLLECTION_NAME).findOne(MongoDBObject("_id"->idTweet)).asInstanceOf[BasicDBObject]

    if(res!=null){
     Some(new Tweet(res))

    }
    else None

  }

  /**
    * Shows a pinWheel in the console.err
    *
    * @param someFuture the future we are all waiting for
    */
  private def showPinWheel(someFuture: Future[_]) {
    // Let the user know something is happening until futureOutput isCompleted
    val spinChars = List("|", "/", "-", "\\")
    while (!someFuture.isCompleted) {
      spinChars.foreach({
        case char =>
          Console.err.println(char)
          Thread sleep 200
          Console.err.println("\b")
      })
    }
    Console.err.println("")
  }

  /**
    *
    */
  def findAllTweets() = {

    val collection: MongoCollection = MongoCLientSingleton.clientMongo(MONGO_DB_NAME)("tweets")

    val allDocs: MongoCursor = collection.find

    /*
    val cursors: mutable.Buffer[Cursor] = collection.parallelScan(new ParallelScanOptions(numCursors = 10000, batchSize = 10000, Some(ReadPreference.Primary)))
    /*
        cursors.foreach(cursor=>
          while(cursor.hasNext){
            print(cursor.next())
          }
        )
    */
    // Map each cursor to a future and with each cursor output the doc
    val futureOutput = Future.sequence(
      cursors.map(cursor => {
        val annotator=new DbpediaSpootLightAnnotator

        Future {
         val anns: Iterator[(Long, Option[List[DbpediaAnnotation]])] = for {
            doc <- cursor;
            cleanedText=doc.get("cleaned_text").asInstanceOf[String]
            id=doc.get("_id").asInstanceOf[Long]


          }  yield(id,annotator.annotateText(cleanedText))

          DbpediaAnnotationCollection.inserDbpediaAnnotations(anns)

          Console.out.println( "pippo")
        }

      }))

    showPinWheel(futureOutput)


    */

    //println( allDocs )
    val annotator=new DbpediaSpootLightAnnotator

    var i=0
    for(doc <- allDocs){
      println(i)
      i=i+1
      // println( doc.get("cleaned_text") )
      val annotations=annotator.annotateText(doc.get("cleaned_text").asInstanceOf[String]).getOrElse(List.empty[DbpediaAnnotation])
      val id=doc.get("_id").asInstanceOf[Long]
      DbpediaAnnotationCollection.insertDbpediaAnnotationsOfTweet(id,annotations)
    }
  }

  def main(args: Array[String]) {
    //TweetCollection.findAllTweets()
    /*
   val tweet= TweetCollection.findTweetById(256230354485145600L)
    tweet match{
      case(Some(x))=>print(x)
      case (None)=>println(" id notFound")
    }
  }*/
  val lista=List(1L,2L,3L,4L)

   print(TweetCollection.findMinValueDate())
    //generateCouplesFromList(lista).foreach(println)
  }


}
