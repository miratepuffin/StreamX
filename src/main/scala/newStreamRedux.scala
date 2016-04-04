import java.util

import org.apache.spark._
import scala.collection.mutable
import scala.util.control.NonFatal
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
import org.apache.spark.streaming.dstream.DStream
import java.util.Date
import java.io.{PrintWriter, File}
import scala.collection.mutable.{ArrayBuffer, HashSet}
import java.util.ArrayList
import scala.collection.immutable.List
import java.io._
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.{Path, PathFilter}
import scala.collection.immutable.Map
import org.apache.commons.io.FileUtils
import java.util.concurrent.TimeUnit
import java.nio.file.{Paths, Files}
object newStreamRedux {
  // define the type used to store the list of documents that come in

  val sparkConf = new SparkConf().setAppName("New Stream")
  val sc = new SparkContext(sparkConf)
  //sc.setCheckpointDir("/user/bas30/checkpoint/")
  var batchCount = 0
  
  val conf = sc.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  var vertices: RDD[(VertexId, Long)] = sc.parallelize(Array.empty[(VertexId, Long)])
  var edges: RDD[Edge[String]] = sc.parallelize(Array.empty[Edge[String]])
  val secondGraph: Graph[Long,String] = readGraph("secondCheck/gen")
  var lastIteration = 0L;

  def main(args: Array[String]) {
    while(true){      
      //if(fs.exists(new org.apache.hadoop.fs.Path("/user/bas30/output/"+batchCount))){
      //  processNewFiles()
      //}
      if(Files.exists(Paths.get("additionalProjects/storm-starter/output/"+batchCount))){
        println("true")
        processNewFiles()
      }
    }
  }

  def processNewFiles(){
    println("Batch " + batchCount)
    
    //val batch: RDD[String] = sc.textFile("/user/bas30/output/"+batchCount,10)
    val batch: RDD[String] = sc.textFile("additionalProjects/storm-starter/output/"+batchCount,10)   
    batch.cache()

    val rmvEdgeStrings = batch.filter(string => string.contains("rmvEdge"))
    val rmvNodeStrings = batch.filter(string => string.contains("rmvNode"))
    val addEdgeStrings = batch.filter(string => string.contains("addEdge"))
    val addNodeStrings = batch.filter(string => string.contains("addNode"))

    val rmvEdges: RDD[Edge[String]] = rmvEdgeStrings.map(string => {
      val split = string.split(" ")       
      Edge(split(1).toLong, split(3).toLong, split(2))
    })

    val rmvNodes: RDD[(VertexId,Long)] = rmvNodeStrings.map(string => {
      val id = string.split(" ")(1).toLong
      (id, 1L)
    })    

    val addEdges: RDD[Edge[String]] = addEdgeStrings.map(string => {
      val split = string.split(" ")       
      Edge(split(1).toLong, split(3).toLong, split(2))
    })
    
    val addNodes: RDD[(VertexId,Long)] = addNodeStrings.map(string => {
      val id = string.split(" ")(1).toLong
      (id, 1L)
    })

    val buildTime = System.currentTimeMillis()

    val removedEdges = edges.subtract(rmvEdges)
    val removedNodes = vertices.subtract(rmvNodes)    
    val finalEdgesRemove = (removedEdges.map(v => (v.srcId, v)).union(removedEdges.map(v => (v.dstId, v)))).cogroup(rmvNodes.map(v => (v._1, null))).filter { case (_, (leftGroup, rightGroup)) => !rightGroup.nonEmpty }.map((_._2._1.last)).distinct
    edges = finalEdgesRemove.union(addEdges)
    vertices = removedNodes.union(addNodes)
    
    
    
    edges.cache()
    vertices.cache()
    val mainGraph = Graph(vertices, edges, 1L)
    println("Graphs equal " +graphEqual(mainGraph,secondGraph))
    //if(batchCount % 5 ==0){
    //  edges.checkpoint()
    //  vertices.checkpoint()
    //}
    //mainGraph.checkpoint()    
    //    saveGraph(mainGraph)  
  
    println("Edge Count: " + edges.count)
    println("Vertex Count: " + vertices.count)    
    println("Build time: " + (System.currentTimeMillis() - buildTime))
    println("Batch no:" + batchCount)
    println()
    batchCount = batchCount+1     
  }

  def saveGraph(graph: Graph[VertexId, String]){
    // Write out edges and vertices of graph into file named after current time 
    val timestamp: Long = System.currentTimeMillis //get current time

    graph.vertices.saveAsTextFile("prev/" + timestamp + "/vertices")

    graph.edges.saveAsTextFile("prev/" + timestamp + "/edges")
  }

  def readGraph(savePoint: String): Graph[VertexId, String] = {
    // Return graph for given time
    val vertx = sc.textFile("prev/" + savePoint + "/vertices")

    val vertRDD: RDD[(VertexId, VertexId)] = vertx.map(line => {
      val split = line.split(",") // split the serialized data
      (split(0).substring(1).toLong, 1L) // and turn back into a Vertex
    })

    val edges = sc.textFile("prev/" + savePoint + "/edges")
    val edgeRDD: RDD[Edge[String]] = edges.map(line => {
      val split = line.split(",") // split serialized data

      val src = split(0).substring(5).toLong // extract the src node ID
      val dest = split(1).toLong // destination node Id
      val edg = split(2).substring(0, split(2).length() - 1) // and edge information

      Edge(src, dest, edg) // create new edge with extracted info
    })

    Graph(vertRDD, edgeRDD, 1L) // return new graphs consisting of read in vertices and edges
  }

  def closestGraph(givenTime: Long): String = {
    // Returns time of graph closest to given time
    //val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // set format of time

    //val date = format.parse(givenTime(0) + " " + givenTime(1)).getTime() // turn given time into Unix long
    val date = givenTime //used to do some string stuff, but no point any more
    // Extract list of all graphs in folder prev
    val prevList = new File("prev").listFiles.toList

    // Set starting values for the difference as Maximum 
    var difference = Long.MaxValue

    // and chosen graph as empty string
    var chosen: String = ""

    prevList.foreach(file => {
      val name = file.getName // get the name (unix time created)

      try {
        val fileTime = name.toLong // Turn the name back into a unix time 
        val diff = Math.abs(date - fileTime) // Find the difference to the given time

        // uf the difference is less than current record, set this graph as chosen
        if (diff < difference) {
          difference = diff
          chosen = name
        }

      } catch {
        case e: NumberFormatException => {}
      }
    })

    chosen
  }

  def graphEqual(graph1: Graph[Long,String], graph2: Graph[Long,String]): Boolean = {
    var thisV = graph1.vertices
    var thisE = graph1.edges
    var otherV = graph2.vertices
    var otherE = graph2.edges   
    thisV.count == (thisV.intersection(otherV).count) && thisE.count == (thisE.intersection(otherE).count)
  } 

  def loadStartState(): Graph[Long, String]={
    val nodes: RDD[(VertexId, Long)] = sc.parallelize(Array.empty[(VertexId, Long)])
    val edges: RDD[Edge[String]] = sc.parallelize(Array.empty[Edge[String]])

    Graph(nodes, edges, 1L)
  }

  def status(graph: Graph[VertexId, String]) {
    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)
    println("vertex total: " + graph.numVertices.toInt)
  }
}