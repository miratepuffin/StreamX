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
import java.io._

object snapshotter {
  val sparkConf = new SparkConf().setAppName("Snapshotter")
    
  val sc  = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(15))
  
  var count = 0

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create empty graph
  val initUsers: RDD[(VertexId, Int)] = sc.parallelize(Array.empty[(VertexId, Int)])
  val initEdges: RDD[Edge[(Long, Long)]] = sc.parallelize(Array.empty[Edge[(Long, Long)]])
  var mainGraph = Graph(initUsers, initEdges)

  def main(args: Array[String]) {

    var stream: DStream[String] = null

    args.length match {
      case 2 => stream = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
      case 1 => stream = ssc.textFileStream(args(0))
      case _ => println("Incorrect num of args, please refer to readme.md!")
    }

    extractRDD(stream) // Extract RDD inside the dstream
    
    // Run stream and await termination
    ssc.start()
    ssc.awaitTermination()
  }

  var timestamp = 1L
  def extractRDD(stream: DStream[String]){
    // Put RDD in scope
    stream.foreachRDD(rdd => {
      count = count + 1 // iterations
      println("Iteration " + count)

      if (!rdd.isEmpty) { // causes emtpy collection exception
        timestamp = timestamp + 1L
        // found   : org.apache.spark.graphx.VertexRDD[Int]
        // required: org.apache.spark.rdd.RDD[(org.apache.spark.graphx.VertexId, org.apache.spark.graphx.VertexId)]
        mainGraph = parseCommands(rdd, mainGraph)
      }

      println("End...")
      status(mainGraph)
      saveGraph() // used to save graph, currently off whilst testing, willl probably set some boolean or summin
    })
  }

  def parseCommands(rdd: RDD[String], graph: [Int, (Long, Long)]): Graph[Int, (Long, Long)] {
    val startTime = System.currentTimeMillis

    // reduce the RDD and return a tuple
    var reduced = reduceRDD(rdd)
    var addEdgeSet = reduced._1
    var rmvEdgeSet = reduced._2
    var addNodeSet = reduced._3
    var rmvNodeSet = reduced._4

    val endTime = System.currentTimeMillis - startTime
    println("reduce time = " + endTime)

    val startTime2 = System.currentTimeMillis
    
    mainGraph = graphRemove(mainGraph, rmvEdgeSet, rmvNodeSet)
    mainGraph = graphAdd(mainGraph, addEdgeSet, addNodeSet)

    val endTime2 = System.currentTimeMillis - startTime2
    println("build time = " + endTime2)

    mainGraph
  }

  def graphRemove(
    graph: Graph[Int, (Long, Long)], 
    rmvEdgeSet: HashSet[String], 
    rmvNodeSet: HashSet[String]): Graph[Int, (Long, Long)] = {

    var edges: RDD[Edge[(Long, Long)]] = graph.edges.map(edge => checkVertex(checkEdge(edge, rmvEdgeSet), rmvNodeSet))

    Graph(graph.vertices, edges)
  }

  def checkEdge(edge: Edge[(Long, Long)], rmvEdgeSet: HashSet[String]): Edge[(Long, Long)] = {
    var newEdge = edge
    
    rmvEdgeSet.foreach(string => {
      val split = string.split(" ")
      val srcId = split(1).toLong
      val dstId = split(3).toLong

      if((edge.srcId == srcId) && (edge.dstId == dstId)) {
        println("Found the Edge in edge checker!")
        val attr = edge.attr.asInstanceOf[(Long, Long)]
        if(attr._2 == Long.MaxValue)
          newEdge = Edge(srcId, dstId, (attr._1, timestamp)) 
        else {
          newEdge = Edge(srcId, dstId, (attr._1, attr._2))
          println("ERROR DONE GOOFED IN THE SHIZZLE MA EDGE NIZZLE.")
        }
      } 
    })
    
    newEdge
  }

  def checkVertex(edge: Edge[(Long, Long)], rmvNodeSet: HashSet[String]): Edge[(Long, Long)] = {
    var newEdge = edge
    
    rmvNodeSet.foreach(string => {
      var id = string.split(" ")(1).toLong
      
      if(edge.srcId == id || edge.dstId == id) {
        if(edge.attr._2 == Long.MaxValue)
          newEdge = Edge(edge.srcId, edge.dstId, (edge.attr._1, timestamp)) 
      }
      else {
        newEdge = Edge(edge.srcId, edge.dstId, (edge.attr._1, edge.attr._2))
      }
    })
    
    newEdge 
    // Edge(6, 1, (3, Infinity)) => Edge(6, 1, (3, 5))
    // Edge(2, 6, (4, Infinity)) => Edge(2, 6, (4, 6))
  }

  def graphAdd(
    graph: Graph[Int, (Long, Long)], 
    addEdgeSet: HashSet[String], 
    addNodeSet: HashSet[String]): Graph[Int, (Long, Long)] = {

    var edgeArray: Array[Edge[(Long, Long)]] = addEdgeSet.toArray.map(edge => {
      var command = edge.split(" ")
      Edge(command(1).toLong, command(3).toLong, (timestamp, Long.MaxValue))
    })

    var vertArray = addNodeSet.toArray.map(vert => {
      var command = vert.split(" ")
      (command(1).toLong, 1)
    })

    var vertices = sc.parallelize(vertArray)
    var edges = sc.parallelize(edgeArray)

    // reduces down to distinct based on edge attribute
    Graph(
      graph.vertices.union(vertices), 
      graph.edges.union(edges)
      .map(x => ((x.srcId, x.dstId, x.attr._2), x))
      .reduceByKey((a, b) => if(a.attr._1 < b.attr._1) a else b)
      .map(_._2)
    )
  }

  def saveGraph(){
    mainGraph.edges.foreach(println(_))

    // mainGraph.vertices.saveAsTextFile("prev/" + timestamp.toString + "/vertices")

    // mainGraph.edges.saveAsTextFile("prev/" + timestamp.toString + "/edges")

    // MODIFY THIS
    // val nullWritableClassTag = implicitly[ClassTag[NullWritable]]
    // val textClassTag = implicitly[ClassTag[Text]]
    // val r = this.mapPartitions { iter =>
    //   val text = new Text()
    //   iter.map { x =>
    //     text.set(x.toString)
    //     (NullWritable.get(), text)
    //   }
    // }
    // RDD.rddToPairRDDFunctions(r)(nullWritableClassTag, textClassTag, null)
    //   .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  // def readGraph(range: (Long, Long)): Graph[VertexId, String] = {
  //   for (i <- range._1 to range._2) {
  //     val vertx = sc.textFile("prev/" + i.toString + "/vertices")

  //     val vertRDD: RDD[(VertexId, VertexId)] = vertx.map(line => {
  //       val split = line.split(",") // split the serialized data
  //       (split(0).substring(1).toLong, 1L) // and turn back into a Vertex
  //     })

  //     val edges = sc.textFile("prev/" + i.toString + "/edges")
  //     val edgeRDD: RDD[Edge[String]] = edges.map(line => {
  //       val split = line.split(",") // split serialized data

  //       val src = split(0).substring(5).toLong // extract the src node ID
  //       val dest = split(1).toLong // destination node Id
  //       val edg = split(2).substring(0, split(2).length() - 1) // and edge information

  //       Edge(src, dest, edg) // create new edge with extracted info
  //     })

  //     Graph(vertRDD, edgeRDD, 0L) // return new graphs consisting of read in vertices and edges
  //   }
  // }

  def closestGraph(givenTime: Array[String]): String = {
    // Returns time of graph closest to given time
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // set format of time

    val date = format.parse(givenTime(0) + " " + givenTime(1)).getTime() // turn given time into Unix long

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

  def reduceRDD(rdd: RDD[String]): (HashSet[String], HashSet[String], HashSet[String], HashSet[String]) = {
    // use hashset for its distinct items
    var addEdgeSet = new HashSet[String]()
    var addNodeSet = new HashSet[String]()
    var rmvEdgeSet = new HashSet[String]()
    var rmvNodeSet = new HashSet[String]()

    val rddArray = rdd.collect()

    for (i <- (rddArray.length - 1) to 0 by -1) {
      val split = rddArray(i).split(" ")

      val command = split(0)
      val src = split(1)
      var msg = ""
      var dst = ""

      if(split.length > 2) {
        msg = split(2)
        dst = split(3)
      }

      command match {
        case "addEdge" => {
          if (rmvNodeSet.contains("rmvNode " + src)) { // check if the src is removed lower down
            if (!rmvNodeSet.contains("rmvNode " + dst)) { // check if the dst is also removed, if not
              addNodeSet.add("addNode " + dst)
            }
          } else if (rmvNodeSet.contains("rmvNode " + dst)) { // check if the dst is removed lower down
            addNodeSet.add("addNode " + src)
          } else if (rmvEdgeSet.contains("rmvEdge " + src + " " + msg + " " + dst)) {
            addNodeSet.add("addNode " + src) //no need to check if they are negated as it is checked above
            addNodeSet.add("addNode " + dst)
          } else { //if there are no remove nodes or edges then we can add the command to the subset
            addEdgeSet.add(rddArray(i))
          }
        }
        case "rmvEdge" => {
          if(!addEdgeSet.contains("addEdge " + src + " " + msg + " " + dst) &&
             !rmvNodeSet.contains("rmvNode " + src) && !rmvNodeSet.contains("rmvNode " + dst)) {
            rmvEdgeSet.add(rddArray(i))
          }  
        }
        case "addNode" => if (!rmvNodeSet.contains("rmvNode " + src)) addNodeSet.add(rddArray(i))
        case "rmvNode" => rmvNodeSet.add(rddArray(i)) // rmvNode  can't be contra
        case _ = println("The operation " + command + " isn't valid.")
      }
    }

    (addEdgeSet, rmvEdgeSet, addNodeSet, rmvNodeSet)
  }

  def graphEqual(graph1: Graph[Int, (Long, Long)], graph2: Graph[Int, (Long, Long)]): Boolean = {
    var thisV = graph1.vertices
    var thisE = graph1.edges

    var otherV = graph2.vertices
    var otherE = graph2.edges

    thisV.count == (thisV.intersection(otherV).count) && thisE.count == (thisE.intersection(otherE).count)
  } 

  def status(graph: Graph[Int, (Long, Long)]) {
    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)

    println("vertex total: " + graph.numVertices.toInt)
  }
}