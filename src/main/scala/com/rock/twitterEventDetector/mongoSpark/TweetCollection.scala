package com.rock.twitterEventDetector.mongoSpark

import com.mongodb.casbah._

import scala.concurrent.Future

/**
  * Created by rocco on 07/02/16.
  */
object TweetCollection {

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
    TweetCollection.findAllTweets()
  }

}
