name := "Program Number 1"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=Seq(
 "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
 "org.apache.spark" %% "spark-sql" % "2.0.1",
 "org.apache.spark" % "spark-graphx_2.10" % "2.1.0"
)