package com.rock.twitterEventDetector.dbscanScala;/*
package dbscanScala;

*/
/*
 * JavaWordCount.java
 * Written in 2014 by Sampo Niskanen / Mobile Wellness Solutions MWS Ltd
 * 
 * To the extent possible under law, the author(s) have dedicated all copyright and
 * related and neighboring rights to this software to the public domain worldwide.
 * This software is distributed without any warranty.
 * 
 * See <http://creativecommons.org/publicdomain/zero/1.0/> for full details.
 *//*


import com.rock.twitterFlashMobDetector.dbscanClustering.ClusteringInstance;
import com.rock.twitterFlashMobDetector.dbscanClustering.CoordinateInstance;
import com.rock.twitterFlashMobDetector.dbscanClustering.sparkUtils.SparkMongoIntegration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

public class DbscanSparkObj<T extends ClusteringInstance<T>> implements Serializable {

 
 	*/
/**
	 * 
	 *//*

	private static final long serialVersionUID = -5891137347520367610L;
	private static final Integer NOISE = -2;
	private Double eps;
	private Integer minPts;
	private String executionName;
	private Map<Long, T> clusteredObjects;
	
	*/
/**
	 * 
	 * @param executionName
	 * @param eps
	 * @param minPts
	 *//*

	public DbscanSparkObj(String executionName, Double eps, Integer minPts){
		this.executionName=executionName;
		this.eps=eps;
		this.minPts=minPts;
	}
	*/
/*s
	public <T extends ClusteringInstance<T>> void run(JavaPairRDD<Long,ClusteringInstance<T>> pairRDDtoCluster){
		
	}*//*

	public     void run(JavaSparkContext sc,JavaPairRDD<Long, T> objectsToCluster){
		
 

 		 
		objectsToCluster.count();
		objectsToCluster.cache();
 		
		
		 
		
		 Double eps=1.16726175299287;
		 	JavaPairRDD<Long, Long> neighs=objectsToCluster.cartesian(objectsToCluster).
		 			filter(x->x._1._1.compareTo(x._2._1)<0).
		 			filter(x->x._1._2.distance(x._2._2)<=eps).
		 			flatMapToPair(x->Arrays.asList(new Tuple2<Long,Long>(x._1._1,x._2._1),new Tuple2<Long,Long>(x._2._1,x._1._1)));
		neighs.persist(StorageLevel.MEMORY_ONLY());
		System.out.println(neighs.count());
		*/
/**
		 * avenndo un rdd di coppie di oggetti (x,y) dove d(x,y)<=eps posso già
		 * sapere quali sono i core objects
		 *//*


		
		clusteredObjects = objectsToCluster.collectAsMap();

		JavaPairRDD<Long, List<Long>> neighborhoods = neighs.combineByKey((
				Long x) -> {
			List<Long> list = new LinkedList<Long>();
			list.add(x);
			return list;
		}, (List<Long> accum, Long value) -> {

			accum.add(value);
			return accum;
		}, (List<Long> acc1, List<Long> acc2) -> {
			List<Long> neis = acc1;
			neis.addAll(acc2);
			return neis;
		});
		
		neighborhoods.cache();
		System.out.println(neighborhoods.count());
		dbscanClusteringBatch(sc, objectsToCluster, neighborhoods,minPts,eps);
		
		
		saveClusterResultsToMongoCollection(sc, neighborhoods);
		SparkMongoIntegration.saveNeighs(sc, neighs, executionName);
	}
		 
	
	 

	 
	 

	public static void main(String[] args) { 
	String logFile = "./resource/AggregationDatasetstep1-8.txt"; // Should be some file on your system
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[16]").set("spark.executor.memory","1g");
		//SparkConf conf = new SparkConf().setAppName("Simple Application");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile(logFile);
		//  JavaRDD<Integer> lineLengths = lines.map(s -> s.length());

		// JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2<String, Integer>(s, 1));
		     @SuppressWarnings("resource")
			JavaPairRDD<Long, CoordinateInstance> coordianteInstances = lines.mapToPair(s ->{ String[] splits=s.split("\t");
			 
			Long id=Long.parseLong(splits[0]);
		  //  System.out.println(id);
		     CoordinateInstance  c=new CoordinateInstance(Double.parseDouble(splits[1]), Double.parseDouble(splits[2]));
		     c.setId(id);
		     return new Tuple2<Long, CoordinateInstance>(id,c); } );
		     coordianteInstances.cache();
		     Long startTimeEx = System.currentTimeMillis();
		     DbscanSparkObj<CoordinateInstance> dbscan=new DbscanSparkObj<CoordinateInstance>("cordInstnewSparkww", 1.16726175299287,4);
		     dbscan.run(sc, coordianteInstances);
		
		 
		Long endTimeDate = System.currentTimeMillis();
		Long timeExe = endTimeDate - startTimeEx;
		System.out.println("tempo esecuzione " + timeExe);

	}
	*/
/**
	 * 
	 * @param sc
	 * @param annotatedTweets
	 * @param neighborhoods
	 * @param minPts
	 * @param eps
	 *//*

	public   void dbscanClusteringBatch(JavaSparkContext sc,
			JavaPairRDD<Long, T> annotatedTweets,
			JavaPairRDD<Long, List<Long>> neighborhoods,Integer minPts,Double eps) {

	
		
		
		int exec = 0;
		int currentCluster = 1;
		// neighborhoods.filter(tuple->tuple._2.size()<minPts).join(annotatedTweets).foreach(tuple->tuple._2._2.setCluster(NOISE));

		JavaPairRDD<Long, List<Long>> coreObjects = neighborhoods
				.filter(tuple -> tuple._2.size() >= minPts);
		Map<Long, List<Long>> coreObjectsMap = coreObjects.collectAsMap();
		for (Entry<Long, T> tweetEntry : clusteredObjects.entrySet()) {
			if (!coreObjectsMap.containsKey(tweetEntry.getKey())) {
				tweetEntry.getValue().setCluster(NOISE);
			}
		}
		Long numberOfCoreObjects=coreObjects.count();
		ArrayList<Integer> initialClusterIds=new ArrayList<>(numberOfCoreObjects.intValue());
		Broadcast<ArrayList<Integer>> broadCastClusters = sc.broadcast(initialClusterIds);
        coreObjects.map(core->{
            return null;
        });

		for (Entry<Long, List<Long>> entry : coreObjectsMap.entrySet()) {

			T currentInstance = clusteredObjects.get(entry.getKey());
			if (currentInstance.getCluster() == -1) {
				System.out.println("sto espandendo il core object "
						+ currentInstance.getId());
				exec++;
				System.out.println("esecyz " + exec);
				 expandCoreObject(coreObjectsMap,currentInstance, currentCluster);
				currentCluster++;

			}
		}
		
		System.out.println("NUMERO CORE OBJECTS " + coreObjectsMap.size());
		System.out.println("NUMERO di tweets " + clusteredObjects.size());

		System.out.println("NUMERO ESECUZIONI " + exec);
		System.out.println("NUMERO CLUSTER " + currentCluster);
		
		

	}
	
	*/
/**
	 * 
	 * @param sc
	 * @param neighborhoods
	 *//*

	private void saveClusterResultsToMongoCollection(JavaSparkContext sc,JavaPairRDD<Long, List<Long>> neighborhoods){
		List<Tuple2<Long, BSONObject>> listaPair = new LinkedList<Tuple2<Long, BSONObject>>();
		for (Entry<Long, T> entry : clusteredObjects.entrySet()) {
			System.out.println(entry.getValue().getCluster());

			BSONObject bson = new BasicBSONObject();
			bson.put("cid", entry.getValue().getCluster());
 
 
			listaPair.add(new Tuple2<Long, BSONObject>(entry.getKey(), bson));
		}

		 
		JavaPairRDD<Long, BSONObject> savesCl = sc.parallelizePairs(listaPair);
		savesCl = savesCl.leftOuterJoin(neighborhoods).mapToPair(t -> {
			BSONObject o = t._2._1;
			if(t._2._2.isPresent())
				o.put("nc", t._2._2.get().size());
			else 
				o.put("nc", 0);
			return new Tuple2<Long, BSONObject>(t._1, o);
		});
		SparkMongoIntegration.saveClusteredTweetsExecution(sc, savesCl,executionName);
	}

	*/
/**
	 *
	 * @param coreObjectsMap
	 * @param currentInstance
	 * @param currentCluster
     *//*

	private void expandCoreObject(
			Map<Long, List<Long>> coreObjectsMap,
			 
			T currentInstance, Integer currentCluster) {
		// TODO Auto-generated method stub
		List<Long> idNeighs = coreObjectsMap.get(currentInstance.getId());
		System.out.println(currentInstance);
		currentInstance.setCluster(currentCluster);
		Stack<T> seeds = new Stack<T>();
		for (Long idNeigh : idNeighs) {
			T neigh = this.clusteredObjects.get(idNeigh);

			neigh.setCluster(currentCluster);
			seeds.add(neigh);
		}

		while (!seeds.empty()) {
			T currentP = seeds.pop();

			currentP.setVisited(true);
			*/
/**
			 * se currentP è un core object
			 *//*

			if (coreObjectsMap.containsKey(currentP.getId())) {
				List<Long> currentPNeighboroodIds = coreObjectsMap
						.get(currentP.getId());
				currentP.setNepsCardinality(currentPNeighboroodIds.size());

				for (Long currentId : currentPNeighboroodIds) {
					T resultP = clusteredObjects.get(currentId);
					*/
/**
					 * resultP.nepsCardinality=this.distanceMatrix.
					 * getEpsNeighborood(resultP, eps).size(); con questa
					 * istruzione
					 *//*

					if (resultP.getCluster() == -2
							|| resultP.getCluster() == -1) {

						if (resultP.getCluster() == -1) {
							seeds.push(resultP);
						}
						resultP.setCluster(currentCluster);

					}// END IF unclassified or noise

				}// end for

			}
		}
 
	}
}*/
