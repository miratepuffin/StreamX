name := "StreamX"

version := "2.0"
scalaVersion := "2.11.4"



libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies +="org.apache.spark" % "spark-streaming_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0" % "provided"
