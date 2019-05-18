
// Reza Marzban

//Graph Based Processing
//Twitter Keywors used: spring, april, nature.

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, IntegerType,LongType,StringType,DoubleType}

object Reza_Marzban_Program_1 {
 def main(args:Array[String]){
 val conf=new SparkConf().setAppName("Program Number 1")
 val sc=new SparkContext(conf)
 sc.setLogLevel("ERROR")
 //creates Spark Session
 val spark = SparkSession.builder().appName("Program Number 1").getOrCreate()
 import spark.implicits._
 //tweets folder address on HDFS server -  ignore files with .tmp extensions (Flume active files).
 val inputpath="hdfs://hdfs input address"
 val outputpath="hdfs://hdfs output address"
 //get the raw tweets from HDFS
 val raw_tweets= spark.read.format("json").option("inferScehma","true").option("mode","dropMalformed").load(inputpath)
 //get the user ID, User Name, tweet text, and timestamp in the format of miliseconds from the raw data. and sort it according to time. text is transformed to lower case.
 val tweets=raw_tweets.select(functions.col("user.id_str").cast(LongType).as("id_str"),functions.col("user.name").as("userName"),functions.lower(functions.col("text")) ,raw_tweets("timestamp_ms").cast(LongType).as("timestamp_ms")).sort("timestamp_ms")
 //select the tweets that contain one of three keywords : spring, april, nature.
 val key1=tweets.select(functions.col("id_str"),functions.col("userName"),functions.col("timestamp_ms")).filter(functions.col("text").contains("spring"))
 val key2=tweets.select(functions.col("id_str"),functions.col("userName"),functions.col("timestamp_ms")).filter(functions.col("text").contains("april"))
 val key3=tweets.select(functions.col("id_str"),functions.col("userName"),functions.col("timestamp_ms")).filter(functions.col("text").contains("nature"))
 //get the distinct user ID's from the users that at least used one keyword in their tweets to create nodes.
 val nodes=tweets.select(functions.col("id_str"),functions.col("userName")).withColumnRenamed("id_str","id").distinct()
 
 //get the timestamp difference of each tweet that has keyword1
 val key1join = key1.alias("t1").join(key1.alias("t2"), functions.col("t1.timestamp_ms") =!= functions.col("t2.timestamp_ms"), "leftOuter").toDF("id_str","userName", "timestamp_ms", "id_str2","userName2", "timestamp_ms2")
 val key1diff =key1join.withColumn("diff",functions.col("timestamp_ms2")-functions.col("timestamp_ms"))
 //choose the ones that has difference less than 15 minutes
 val key1diff15=key1diff.filter(functions.col("diff")<900001).filter(functions.col("diff")>1).select(functions.col("id_str"),functions.col("id_str2"))
 //get the timestamp difference of each tweet that has keyword2
 val key2join = key2.alias("t1").join(key2.alias("t2"), functions.col("t1.timestamp_ms") =!= functions.col("t2.timestamp_ms"), "leftOuter").toDF("id_str","userName", "timestamp_ms", "id_str2","userName2", "timestamp_ms2")
 val key2diff =key2join.withColumn("diff",functions.col("timestamp_ms2")-functions.col("timestamp_ms"))
 //choose the ones that has difference less than 15 minutes
 val key2diff15=key2diff.filter(functions.col("diff")<900001).filter(functions.col("diff")>1).select(functions.col("id_str"),functions.col("id_str2"))
 //get the timestamp difference of each tweet that has keyword3
 val key3join = key3.alias("t1").join(key3.alias("t2"), functions.col("t1.timestamp_ms") =!= functions.col("t2.timestamp_ms"), "leftOuter").toDF("id_str","userName", "timestamp_ms", "id_str2","userName2", "timestamp_ms2")
 val key3diff =key3join.withColumn("diff",functions.col("timestamp_ms2")-functions.col("timestamp_ms"))
 //choose the ones that has difference less than 15 minutes
 val key3diff15=key3diff.filter(functions.col("diff")<900001).filter(functions.col("diff")>1).select(functions.col("id_str"),functions.col("id_str2"))
 //create the edges from 3 different keywords and renaming the columns into right format
 val edges=key1diff15.union(key2diff15).union(key3diff15).withColumnRenamed("id_str","src").withColumnRenamed("id_str2","dst")
 spark.conf.set("spark.sql.shuffle.partitions",5)
 //convert nodes and edges into GraphX format.  
 val GraphxNodes: RDD[(VertexId, String)] = nodes.select("id","userName").rdd.map(row => (row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[String])) 
 val GraphxEdges:RDD[Edge[Long]] = edges.select("src", "dst").rdd.map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1))
 //create Graphx with nodes and edges. set the default node to "00000".
 val defaultNode = ("00000")
 val twitterGraph = Graph(GraphxNodes, GraphxEdges, defaultNode)
 //save graph to memory for later analysis
 twitterGraph.cache()

 //a.
 //get the out degree of vertices
 val rows = twitterGraph.outDegrees.map{case(id, v) => Row.fromSeq(Seq(id, v))}
 val schema = StructType(List(StructField("id", LongType, false) ,StructField("prop", IntegerType, false)))
 val outdegree = spark.createDataFrame(rows, schema).select(functions.col("id"))
 // find vertices with zero out degree (no outdegree means zero outdegree)
 val rows1 = twitterGraph.vertices.map{case(id, v) => Row.fromSeq(Seq(id, v))}
 val schema1 = StructType(List(StructField("id", LongType, false) ,StructField("Name", StringType, false)))
 val zeroOutdegree=spark.createDataFrame(rows1, schema1).select(functions.col("id")).except(outdegree).as[Long].collect()
 //get the shotest path to each and every node with zero outdegree 
 val result = ShortestPaths.run(twitterGraph, zeroOutdegree)
 //find the maximum number of hops to reach a node with zero out degree
 val r = result.vertices.map(row => {
 row._2.toSeq.map(x => (row._1, x._1, x._2))
 val v = row._1
 val a = row._2.valuesIterator.max
 val d = (v,a)
 d
 })
 val rows2 = r.join(GraphxNodes).map{case(id, (influ, username)) => Row.fromSeq(Seq(id, username, influ))}
 val schema2 = StructType(List(StructField("id", LongType, false) ,StructField("userName", StringType, false),StructField("Influence", IntegerType, false)))
 val UserInfluence = spark.createDataFrame(rows2, schema2).sort(functions.desc("Influence"))
 //saving the UserInfluence in HDFS in the format of ID UserInfluence.
 UserInfluence.rdd.repartition(1).saveAsTextFile(outputpath.concat("UserInfluenceOutput"))
 
 //b.
 //creating the PageRank data frame.
 val ranks = twitterGraph.pageRank(0.0001).vertices
 val rows3 = ranks.join(GraphxNodes).map{case(id, (pagerank, username)) => Row.fromSeq(Seq(id, username, pagerank))}
 val schema3 = StructType(List(StructField("id", LongType, false) ,StructField("userName", StringType, false),StructField("PageRank", DoubleType, false)))
 val ranknodes=spark.createDataFrame(rows3, schema3).sort(functions.desc("PageRank"))
 println(" ")
 //saving the pagerank in HDFS in the format of ID Username PageRank.
 ranknodes.rdd.repartition(1).saveAsTextFile(outputpath.concat("PageRankOutput"))
 println("All results saved to HDFS!")
 //printing results
 println("A.Printing top 20 users with highest Influence: ")
 UserInfluence.show(20,false)
 println("B.Printing top 20 users with highest PageRank: ")
 ranknodes.show(20,false)
 println("Total Number of users (nodes): " + twitterGraph.numVertices)
 println("Total Number of relations (edges): " + twitterGraph.numEdges)
 println(" ")
 }
}