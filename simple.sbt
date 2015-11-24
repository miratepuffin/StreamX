name := "StreamX"

version := "2.0"
scalaVersion := "2.10.4"



libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.1"
libraryDependencies +="org.apache.spark" % "spark-streaming_2.10" % "1.5.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.5.1" % "provided"
