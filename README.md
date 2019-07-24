# Twitter-Graphx-Spark
creating a graph with Graphx and RDD based library in spark

Step 1. Stream twitter data on three specific keywords using Apache Flume.

Step 2. Create a spark dataframe and clean them and finally convert them to rdds.

Step 3. Create a graph with Graphx RDD based library.

Step 4. get the pagerank of each user.

Step 5.  get the influence of each user. (influence of a user is measured by max number of hops it takes to reach a vertex(user) without any outdegrees).

__________________________________________
Twitter Keywors used: spring, april, nature.

How To Run Reza_Marzban_Program_1.scala:

	1- On the spark cluster copy Reza_Marzban_Program_1.scala on folder structure: /src/main/scala/Reza_Marzban_Program_1.scala
	2- Create a file named "build.sbt" with following content:
name := "Program Number 1"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=Seq(
 "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
 "org.apache.spark" %% "spark-sql" % "2.0.1",
 "org.apache.spark" % "spark-graphx_2.10" % "2.1.0"
)

	3- Copy build.sbt on your root directory beside src folder.
	4- Enter this Command: sbt clean
	5- Enter this Command: sbt package
	6- Enter this Command: spark-submit --class Reza_Marzban_Program_1 ./target/scala-2.10/program-number-1_2.10-1.0.jar
	

The output files are attached. 
UserInfluenceOutput.txt pattern:   [userId,userName,Influence]
PageRankOutput.txt pattern:   [userId,userName,PageRank]
