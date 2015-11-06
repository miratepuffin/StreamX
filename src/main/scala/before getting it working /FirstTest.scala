import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
object Main {
  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    }
      Logger.getLogger("org").setLevel(Level.OFF)
  	  Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Load graph")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(SparkContext.jarOfClass(this.getClass).toList)

    val sc = new SparkContext(conf)

    val edges: RDD[Edge[String]] =
      sc.textFile(args(0)).map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(2).toLong, fields(1))
      }

    val graph : Graph[Any, String] = Graph.fromEdges(edges, "defaultProperty")


    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);
  }
}