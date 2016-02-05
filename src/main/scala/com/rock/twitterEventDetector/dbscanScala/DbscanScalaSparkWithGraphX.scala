package com.rock.twitterEventDetector.dbscanScala

import scala.collection.{Map, mutable}

/**
  * Created by rocco on 23/01/2016.
  */
class DbscanScalaSparkWithGraphX (data : RDD[(Long, CoordinateInstance)] = null, executionName:String, minPts: Int =4, eps:Double=1.117 ) extends Serializable {


  def isRoot(id:Long,start:Map[Long,List[Long]]): Boolean ={
    start  map{
      case (idNode,childrensList)=>if (childrensList.contains(id)) {
        return false
      }
    }
    return true
  }


  def run(sparkContext: SparkContext): VertexRDD[VertexId] = {
    object SetAccParam extends AccumulableParam[mutable.HashSet[Long], Long] {
      override def addAccumulator(r: mutable.HashSet[Long], t: Long): mutable.HashSet[Long] = {
        r.add(t)
        r
      }

      override def addInPlace(r1: mutable.HashSet[Long], r2: mutable.HashSet[Long]): mutable.HashSet[Long] = r1 ++ r2

      override def zero(initialValue: mutable.HashSet[Long]): mutable.HashSet[Long] = new mutable.HashSet[Long]()
    }

    val idAcc = sparkContext.accumulable(new mutable.HashSet[Long]())(SetAccParam)
    data.cache()
    // val neighs: RDD[(Long, Long)] =
    // val idAcc = sparkContext.accumulator(new  mutable.HashSet[Long] )(SetAccParam)


    val neighRDD: RDD[(Long, Long)] = data.cartesian(data)
      .filter { case (a, b) => a._1 < b._1 }

      .filter(x => x._1._2.distance(x._2._2) <= eps)
      .flatMap(x => List((x._1._1, x._2._1), (x._2._1, x._1._1)))

   // val group: RDD[(Long, Iterable[Long])] = neighRDD.groupByKey();
    val ris: RDD[(VertexId, VertexId)] =  neighRDD.groupByKey().filter(x=>x._2.size>minPts).flatMap {
      case (idCore:Long, listNeighbor:Iterable[Long]) => listNeighbor map{ neighbor=>(idCore,neighbor)

      }
    }
    ris.collect().foreach(x=>println(x._1+" "+x._2))


    val graph: Graph[Int, Int] = Graph.fromEdgeTuples(ris, 1)
    val connectedComponents: VertexRDD[VertexId] = graph.connectedComponents().vertices;
    connectedComponents

    // val clusteredData: RDD[(Long, (CoordinateInstance, Long))] =data.join(connectedComponents);

  }


}
