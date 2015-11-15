import org.apache.spark._
import scala.util.control.NonFatal
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.ReceiverInputDStream

object DeleteTest {
  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))
  var count = 0

  //create the context from which you are streaming
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  //turn off the 100's of messages so you can see the output

  var myGraph : Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(Array[Edge[String]]()),0L) 
  //create empty graph

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    } //requires you to specify a IP/port from which the data is incoming (localhost 9999 for example)

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    
    extractRDD(lines)

    ssc.start()
    ssc.awaitTermination()
  }

  def extractRDD ( lines: ReceiverInputDStream[String] ) {
    lines.foreachRDD( rdd => {
      count=count+1
      if (!rdd.partitions.isEmpty){ //check if the partition is empty (no new data) as otherwise we get empty collection exception
        rdd.foreach {line => actionCheck(line.split(" "))} //otherwise we compute the new graph and then unionise it with the previous
      }
      doStuff()
      saveGraph()
    })
  }

  def actionCheck(fields: Array[String]){
    if(fields(0)=="addEdge"){
      addEdge(Edge(fields(1).toLong, fields(3).toLong, fields(2)))
    }
    else if (fields(0)=="addNode"){
      addNode(fields(1))
    }
    else if (fields(0)=="rmvEdge"){
      removeEdge(fields)
    }
    else if (fields(0)=="rmvNode"){
      removeNode(fields(1))
    }
  }


  def addEdge(newedge: Edge[String]){
    val newgraph : Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(Array(newedge)), 1L)
    myGraph = Graph(myGraph.vertices.union(newgraph.vertices), myGraph.edges.union(newgraph.edges),0)
    
  }
  def addNode(newNode: String){
    val id: Array[(VertexId,VertexId)] = Array((newNode.toLong,1))
    val idRDD: RDD[(VertexId,VertexId)] = sc.parallelize(id)
    myGraph = Graph(myGraph.vertices.union(idRDD), myGraph.edges,0)
  }

  def removeEdge(fields: Array[String]){
    myGraph = myGraph.subgraph(epred => checkEdge(fields,epred)) 
  }
  def checkEdge(fields: Array[String],epred:EdgeTriplet[VertexId,String]):Boolean={
    if((epred.srcId != (fields(1).toLong)) && (epred.dstId != (fields(3).toLong)) && (epred.attr != (fields(2)))) true
    else false
  }

  def removeNode(nodeName: String){
    myGraph = myGraph.subgraph(vpred = (vid, attr) => vid != nodeName.toLong ) 
  }

  def saveGraph(){
    val timestamp: Long = System.currentTimeMillis
    myGraph.triplets.saveAsTextFile("prev/"+timestamp)
  }

  def doStuff(){
    println("Preforming batch operation...")
    myGraph.triplets.map(triplet => triplet.srcId + " is the " + triplet.attr + " of " + triplet.dstId).collect.foreach(println(_))
    myGraph.vertices.foreach(id => {println(id)})
  }
}