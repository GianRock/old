package com.rock.twitterEventDetector.dbscanScala.lsh

import java.io.InputStream
import java.util

import com.rock.twitterFlashMobDetector.nlp.indexing.{AnalyzerUtils, MyAnalyzer}
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import nlp.as._
import org.apache.spark.mllib.feature.{HashingTF, IDF, Normalizer}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConverters._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

/**
  * Created by rocco on 17/01/2016.
  */
object tfidfmain {

  def main(args: Array[String]) {
    //init spark context




    val numPartitions = 8
    val dataFile = "tweetsNov2.txt"
    val conf = new SparkConf()
      .setAppName("LSH")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    //read data file in as a RDD, partition RDD across <partitions> cores
    val data = sc.textFile(dataFile, numPartitions)


    // Load documents (one per line).
    val documents: RDD[(Long,Seq[String])] = sc.textFile(dataFile).filter(line=>line.split("\t").size>1).mapPartitions(it=> {
      val stream : InputStream = getClass.getResourceAsStream("/nlp/oov.txt")
      val lines = scala.io.Source.fromInputStream( stream ).getLines

      println("DIM FILE "+lines.length)
      val oovMap: Map[String, String] = lines.map{
        line=> {
          val parts= line.split("\t")
          (parts(0),parts(1))
        }
      }.toMap
      val analyzer=new MyAnalyzer()
      it.map(

        line => {
          val parts = line.split("\t")
          val x = parts.apply(0)

          val text=parts(1)
          val splits: Array[String] =text.split(" ")
          val normalizedText=splits.map{
            token=>{oovMap.getOrElse(token,token)}
          }.mkString(" ")

          if(normalizedText!=text){
            println(" HO NORMALIZZATO \n\r"+text +"\n\r"+normalizedText)
          }
          val se: util.List[String] = AnalyzerUtils.tokenizeText(analyzer,normalizedText);
          (parts.apply(0).toLong, se.asScala)
        })


    })


    println(documents.count())
    val numDictonary=Math.pow(2,18).toInt
    val hashingTF = new HashingTF(numDictonary)
    //documents.map((id,seq)=>(id,hashingTF.transform(seq)))



    val tfVectors:RDD[(Long,Vector)] =documents.map(tuple=>(tuple._1,hashingTF.transform(tuple._2)))


    tfVectors.cache()
    val idf = new IDF(3).fit(tfVectors.values)

    val norm:Normalizer = new Normalizer()
    val tfidf: RDD[(Long,SparseVector)] =tfVectors.map(tuple=>(tuple._1,idf.transform(tuple._2).toSparse))


    val maxNonZeros=tfidf.takeOrdered(1)(Ordering[Int].reverse.on(_._2.numNonzeros)).head

    println(maxNonZeros._1)

    val lsh = new LSH(tfidf,numDictonary, numHashFunc =10, numHashTables = 13)
    val model = lsh.run()


    val modModel: RDD[(Long, Iterable[(Int, String)])] =model.hashTables.map{
      case(hashkey,id)=>(id,hashkey)
    }.groupByKey()
    modModel.cache()
    val indexedModel: IndexedRDD[Long, Iterable[(Int, String)]] = IndexedRDD(modModel)
  val indexedHashTable =model.hashTables.groupByKey();
    indexedHashTable.cache()

    val sampleVector=tfidf.lookup(265595904105537537L).head
    //get the near neighbors of userId: 4587 in the model
    val candList = model.getCandidates(265595904105537537L).collect().toList
    println("Number of Candidate Neighbors: "+candList.size)
    println("Candidate List: " +candList)
    //compute similarity of sampleVector with users in candidate set
    val candidateVectors = tfidf.filter(x => candList.contains(x._1)).cache()
    val similarities:RDD[(Long,Double)]= candidateVectors.map(x => (x._1, lsh.cosine(x._2, sampleVector)))
    val n=similarities.count()
    //
    //
    similarities.takeOrdered(n.toInt)(Ordering[Double].reverse.on(_._2)) foreach println
    // similarities.takeOrdered(n)(Ordering[Double].reverse.on(_._2))
    similarities foreach println



    candList.foreach(long=>println(documents.lookup(long).head))

    val index=hashingTF.indexOf("rommey")
    println("indice di romney"+index)

    //save model
    val temp = "target/" + System.currentTimeMillis().toString
    model.save(sc, temp)
    //load model
    val modelLoaded = LSHModel.load(sc, temp)

    //print out 10 entries from loaded model
    modelLoaded.hashTables.take(15) foreach println
  }
}