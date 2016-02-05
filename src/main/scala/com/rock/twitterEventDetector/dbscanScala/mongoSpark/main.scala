/*
package mongoSpark

import play.api.libs.iteratee.Iteratee
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{collections, DefaultDB}
import reactivemongo.bson.BSONDocument
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext

/**
  * Created by rocco on 05/02/2016.
  */
object main {
  def main(args: Array[String]) {
    val db: DefaultDB = MymongoConnection.conn("tweetEventDataset")
    val collectionTweet =db[BSONCollection]("tweets")
    val query = BSONDocument("_id" -> 256230354485145600L)
    // select only the fields 'lastName' and '_id'
    val filter = BSONDocument(
      "created_at" -> 1,
      "_id" -> 1)

    /* Let's run this query then enumerate the response and print a readable
     * representation of each document in the response */
    collectionTweet.
      find(query, filter).
      cursor()[BSONDocument].
      enumerate().apply(Iteratee.foreach { doc =>
      println(s"found document: ${BSONDocument pretty doc}")
    })

  }
}
*/
