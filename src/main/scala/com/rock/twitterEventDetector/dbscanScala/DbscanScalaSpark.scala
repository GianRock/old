package com.rock.twitterEventDetector.dbscanScala

import scala.collection.{Map, mutable}

/**
  * Created by rocco on 23/01/2016.
  */
class DbscanScalaSpark (data : RDD[(Long, CoordinateInstance)] = null, executionName:String, minPts: Int =4, eps:Double=1.117 ) extends Serializable {


  def isRoot(id:Long,start:Map[Long,List[Long]]): Boolean ={
    start  map{
      case (idNode,childrensList)=>if (childrensList.contains(id)) {
        return false
      }
    }
    return true
  }


  def run(sparkContext: SparkContext) {
    object SetAccParam extends AccumulableParam[mutable.HashSet[Long],Long] {
      override def addAccumulator(r: mutable.HashSet[Long], t: Long): mutable.HashSet[Long] = {
        r.add(t)
        r
      }

      override def addInPlace(r1: mutable.HashSet[Long], r2: mutable.HashSet[Long]): mutable.HashSet[Long] = r1++r2

      override def zero(initialValue: mutable.HashSet[Long]): mutable.HashSet[Long] = new mutable.HashSet[Long]()
    }

    val idAcc = sparkContext.accumulable(new mutable.HashSet[Long]())(SetAccParam)
    data.cache()
    // val neighs: RDD[(Long, Long)] =
    // val idAcc = sparkContext.accumulator(new  mutable.HashSet[Long] )(SetAccParam)


    val neigGraph=data.cartesian(data)
      .filter { case (a, b) => a._1 < b._1 }

      .filter(x => x._1._2.distance(x._2._2) <= eps)
      // .flatMap(x => List((x._1._1, x._2._1), (x._2._1, x._1._1)
      .map( x => (x._1._1, x._2._1) )

    val coreObjects: RDD[(Long, Iterable[Long])] =neigGraph

      .groupByKey().filter(x => x._2.size >= minPts)

    coreObjects.cache()
    println(coreObjects.count())
    val orderedCores: RDD[(Long, Iterable[Long])] =coreObjects.sortBy(x=>x._2.size)
    /**
      * val partialCLusterdData sarà un rdd di coppie (idObject:Long,idCLuster:Long)
      *
      * in cui il primo argomento è l'id dell'oggeto,
      * il secondo è un id cluster temporaneo
      */
    val partialClusererdDataZIPPED= orderedCores.zipWithIndex()

    val partialClusererdData=partialClusererdDataZIPPED
      .flatMap {


        case ((coreObject: Long, neighborhood: Iterable[Long]), clusterIdTemp: Long) =>
          idAcc.add(coreObject)
          neighborhood.map(neighbor => (neighbor, clusterIdTemp))
      }
    println("PARTIAL CLUSTERING COunt "+partialClusererdData.count())

    println("MAX CLUSTER ID" +partialClusererdData.values.max())
    println(" NUM CORE OBJECTS "+coreObjects.count())
    println(" NUM CORE OBJECTS ORDERED "+orderedCores.count())
    val collision: RDD[(Long, Iterable[Long])] = partialClusererdData.
      groupByKey().filter(x => x._2.size > 1).map(x=>(x._1,x._2.toList.sortWith((_ < _))))


    println("COLLISION   "+collision.count())

    val broadCastCore=sparkContext.broadcast(idAcc.value)


    val collisionTemporanyClusterIds:RDD[(Long, Iterable[Long])]=collision.filter(
      x=>broadCastCore.value.contains(x._1)
    ).map(x=>(x._2.head,x._2.tail)).sortByKey()
    /**
      *
      */
    val collisionGrouped: RDD[(Long, Set[Long])] =collisionTemporanyClusterIds.groupByKey().map(x=>(x._1,x._2.flatten.toSet)).sortByKey()
    val newcollisionGrouped=collisionGrouped.map(x=>(x._1,x._2.toList.sorted))
    newcollisionGrouped.collect().foreach(x=>println(x))

    val collisionDisambiguat: RDD[(Long, Iterable[Long])] = newcollisionGrouped.flatMap {


      case ((idCluster: Long, mergeClustersSet: List[Long])) =>

        mergeClustersSet.map(cluster => (cluster, idCluster))
    }.groupByKey()
    val newCollision=collisionDisambiguat.sortByKey().map(x=>(x._1,x._2.toList.sorted))

    newcollisionGrouped.collect().foreach(x=>println(x))

    val start: Map[Long, List[Long]] =newcollisionGrouped.collectAsMap()

    val z:Map[Long, scala.List[Long]]=start.map(x=>(x._1,traverse(start,x._1)))
    println("PRIMA ESPANZIONE ")
    z.foreach(x=>println(x))

    println("RESULT  DOPO ESPANSIONE "+z.size)
    z.foreach(x=>println(x))
    val resultss:Map[Long,List[Long]]=z.filter(x=>{isRoot(x._1,start)})
    println("CLUSTER FINALI "+resultss.size)

    resultss.foreach(x=>println(x))

    /**
      *
      */

    println("NEIGH GRAPH")
    neigGraph.collect().foreach(x=>println(x._1+" "+x._2))
  }

  def traverse(graph: Map[Long, List[Long]], start: Long): List[Long] = {


    def visitNode(idNode:Long):List[Long]=graph.getOrElse(idNode,List.empty[Long])
    var visitedGraph: mutable.HashMap[Long, scala.List[Long]]=new mutable.HashMap[Long,List[Long]]()

    @annotation.tailrec
    def loop(stack: List[Long], visited: List[Long]): List[Long] = {
      if (stack isEmpty) visited
      else {
        if(visitedGraph.isDefinedAt(stack.head)){
          loop(stack.tail,visited++visitedGraph.get(stack.head).get)
        }else {

          loop(visitNode(stack.head) ++ stack.tail, stack.head :: visited)
        }

      }
    }
    val returnedList=loop(List(start), List.empty[Long])
    visitedGraph += (start->returnedList)
    returnedList reverse

  }
}
