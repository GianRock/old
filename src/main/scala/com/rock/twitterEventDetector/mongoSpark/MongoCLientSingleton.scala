package com.rock.twitterEventDetector.mongoSpark

import com.mongodb.casbah.MongoClient
import com.rock.twitterEventDetector.configuration.Constant
/**
  * Created by rocco on 05/02/2016.
  */
object MongoCLientSingleton {
    lazy val clientMongo=MongoClient(Constant.MONGO_URL,27017)
    //MongoClientOptions mo = MongoClientOptions.builder.connectionsPerHost(100).build;
    //val clientMongo= MongoClient(url,port)
}
/*
object MongoCLientSingleton{
   def apply(url:String=MONGO_URL,port:Int=27017)=new MongoCLientSingleton(url,port)
}*/
