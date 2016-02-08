name := "TwitterEventDetectorSpark"

version := "1.0"

scalaVersion := "2.10.6"
resolvers ++= Seq("Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven")

libraryDependencies++=Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2",
  "org.apache.spark" % "spark-mllib_2.10" % "1.5.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2",
  "org.mongodb" % "casbah_2.10" % "3.1.0",
  "org.mongodb" % "mongo-java-driver" % "3.2.1",
  "org.apache.httpcomponents" % "httpcore" % "4.2.4",
  "org.apache.httpcomponents" % "httpclient" % "4.2.4",
  "org.apache.lucene" % "lucene-core" % "5.2.1",
  "org.apache.lucene" % "lucene-analyzers-common" % "5.2.1",
  "org.apache.lucene" % "lucene-queryparser" % "5.2.1"


)


