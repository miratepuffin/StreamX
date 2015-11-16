import org.apache.spark._
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
import java.io.File

object decentParseTest {
  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(15))
  var count = 0
  var innercount = 0

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create empty graph
  var mainGraph : Graph[VertexId, String] = Graph.fromEdges(
    sc.parallelize (Array[Edge[String]]()), 0L
  )

  def main (args : Array[String]) {
    
    var lines : DStream[String] = null

    if (args.length == 1) 
      lines = ssc.textFileStream(args(0))
    
    else if (args.length == 2)
      lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    else {
      // Requires IP/port i.e. (localhost 9999) or file path
      System.err.println("Please input stream ip:port / file path!")
      System.exit(1)
    }

    extractRDD (lines) // Extract RDD inside the dstream

    // Run stream and await termination
    ssc.start() 
    ssc.awaitTermination()
  }

  def extractRDD (lines : DStream[String]) {
    // Put RDD in scope
    lines.foreachRDD (rdd => {
      // Count of itterations run
      count = count+1
      println("Itteration "+count)
      // Check if the partition is empty (no new data)
      // otherwise; causes empty collection exception.
      if(!rdd.isEmpty){
      val tempGraph: Graph[VertexId, String] = Graph (mainGraph.vertices, mainGraph.edges, 0)
      mainGraph = parseCommands (rdd,tempGraph)
      }
      status(mainGraph) // method for print statements etc
      
      //saveGraph() // used to save graph, currently off whilst testing, willl probably set some boolean or summin
    })
  }
  def parseCommands(rdd : RDD[String],tempGraph:Graph[VertexId,String]):Graph[VertexId,String] = {
    if(rdd.isEmpty){
      println("empty")
      //status(tempGraph)
      tempGraph
    }
    else{
      var newrdd = tail(rdd)
      //println(rdd.first)

      val altGraph: Graph[VertexId,String] = performOperation(rdd.first.split(" "),tempGraph)
      //status(altGraph)
      parseCommands(newrdd,altGraph)
    }
  }
  // Map these functions to a HashMap,
  // and use .drop(1) to pass rest of arr
  def performOperation (command : Array[String], tempGraph:Graph[VertexId,String]):Graph[VertexId,String] = {
    // Checks if add edge and passes src, dest and edge.
    if (command(0) == "addEdge") {
      
      addEdge (Edge(
        command(1).toLong, 
        command(3).toLong, 
        command(2)
      ),tempGraph)
    }
    // Checks if add node and passes ID
    else if (command(0) == "addNode") { 
      addNode(command(1),tempGraph)
    }
    // Checks if remove edge, arr. utilised in subgraph check
    else if (command(0) == "rmvEdge") {
      removeEdge(command,tempGraph)
    }
    // Checks if remove node and passes ID
    else if (command(0) == "rmvNode") {
      removeNode(command(1),tempGraph)
    }
    // Checks if loading old graphs and passes date/time
    else if(command(0) == "loadOld") { 
     readGraph(closestGraph (Array(command(1), command(2))))
    }
    else{
        println("reached else")
        tempGraph
      } 
  }

  def addEdge (edge : Edge[String], tempGraph:Graph[VertexId,String]):Graph[VertexId,String] ={
    // Create a new graph with given edge
    val newgraph : Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(Array(edge)), 1L)
    Graph(tempGraph.vertices.union(newgraph.vertices), tempGraph.edges.union(newgraph.edges),0) 
  }

  def addNode (node : String, tempGraph:Graph[VertexId,String]):Graph[VertexId,String] ={
    // Create vertex RDD from ID and union with graph
    val idRDD: RDD[(VertexId, VertexId)] = sc.parallelize (
      Array ((node.toLong, 1)) : Array[(VertexId, VertexId)]
    )
   
    val newNodes = tempGraph.vertices.union(idRDD)

    Graph (newNodes, tempGraph.edges, 0)
  }

  def removeEdge (fields : Array[String], tempGraph:Graph[VertexId,String]):Graph[VertexId,String] ={
    //create subgraph based on given fields
    tempGraph.subgraph (epred => checkEdge(fields, epred)) 
  }

  def checkEdge (fields : Array[String], epred : EdgeTriplet[VertexId,String]) : Boolean = {
    (epred.srcId != (fields(1).toLong)) || 
    (epred.dstId != (fields(3).toLong)) || 
    (epred.attr  != (fields(2)))
  }

  def removeNode (nodeName : String, tempGraph:Graph[VertexId,String]):Graph[VertexId,String] ={
    // Create subgraph where all ID's do not equal given id
    tempGraph.subgraph(vpred = (vid, attr) => vid != nodeName.toLong) 
  }

  def saveGraph () { 
    // Write out edges and vertices of graph into file named after current time 
    val timestamp : Long = System.currentTimeMillis //get current time

    mainGraph.vertices.saveAsTextFile ("prev/"+timestamp+"/vertices")

    mainGraph.edges.saveAsTextFile ("prev/"+timestamp+"/edges")
  }

  def readGraph (savePoint : String):Graph[VertexId,String] = {
    // Return graph for given time
    val vertx = sc.textFile("prev/"+savePoint+"/vertices")

    val vertRDD: RDD[(VertexId,VertexId)] = vertx.map (line => {
      val split = line.split(",") // split the serialized data
      (split(0).substring(1).toLong,1L) // and turn back into a Vertex
    })

    val edges = sc.textFile("prev/"+savePoint+"/edges")
    val edgeRDD: RDD[Edge[String]] = edges.map (line => {
      val split = line.split(",") // split serialized data

      val src  = split(0).substring(5).toLong // extract the src node ID
      val dest = split(1).toLong // destination node Id
      val edg  = split(2).substring(0,split(2).length()-1) // and edge information
      
      Edge(src, dest, edg) // create new edge with extracted info
    })

    Graph(vertRDD,edgeRDD,0L) // return new graphs consisting of read in vertices and edges
  }

  def closestGraph(givenTime : Array[String]):String={ 
    // Returns time of graph closest to given time
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // set format of time

    val date = format.parse(givenTime(0) + " " + givenTime(1)).getTime() // turn given time into Unix long

    // Extract list of all graphs in folder prev
    val prevList = new File("prev").listFiles.toList

    // Set starting values for the difference as Maximum 
    var difference = Long.MaxValue

    // and chosen graph as empty string
    var chosen : String = ""  

    prevList.foreach( file => {
      val name = file.getName // get the name (unix time created)
      
      try {
        val fileTime = name.toLong // Turn the name back into a unix time 
        val diff = Math.abs(date - fileTime) // Find the difference to the given time
        
        // uf the difference is less than current record, set this graph as chosen
        if( diff < difference){ 
          difference = diff 
          chosen = name 
        }

      }catch {
        case e : NumberFormatException => {}
      }
    })

    chosen
  }

  def tail(rdd:RDD[String]):RDD[String] = {sc.parallelize (rdd.collect.tail)}

  def status (graph:Graph[VertexId,String]) {
    println("Performing batch processing...")
    //graph.triplets.map(triplet =>
    //  triplet.srcId + " is the " + 
    //  triplet.attr + " of " + 
    //  triplet.dstId).collect.foreach(println(_)
    //)

    graph.vertices.foreach(id => {println(id)})
  }
}