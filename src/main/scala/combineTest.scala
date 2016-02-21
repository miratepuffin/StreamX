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

object combineTest {
  // define the type used to store the list of documents that come in
  type docList = ArrayList[Tuple6[HashSet[String],HashSet[String],HashSet[String],HashSet[String],Int,Int]] 

  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(1))
  var count = 0
  var countComplete = 0

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create empty graph
  var mainGraph: Graph[VertexId, String] = loadStartState()
  var secondGraph: Graph[VertexId,String] = readGraph("secondCheck/gen")
  def main(args: Array[String]) {

    var lines: DStream[String] = null

    if (args.length == 1)
      lines = ssc.textFileStream(args(0))

    else if (args.length == 2)
      lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)

    else {
      // Requires IP/port i.e. (localhost 9999) or file path
      System.err.println("Please input stream ip:port / file path!")
      System.exit(1)
    }
    extractRDD(lines) // Extract RDD inside the dstream
    // Run stream and await termination
    ssc.start()
    ssc.awaitTermination()
  }

  def extractRDD(lines: DStream[String]){
    // Put RDD in scope
    println()
    lines.foreachRDD(rdd => {
      status(secondGraph)
      // Count of itterations run
      while(count>countComplete){
        println("Waiting for last batch")
      }
      count = count + 1
      println("Itteration " + count)
      // Check if the partition is empty (no new data)
      // otherwise; causes empty collection exception.
      if (!rdd.isEmpty) {
        val tempGraph: Graph[VertexId, String] = Graph(mainGraph.vertices, mainGraph.edges, 0)
        mainGraph = parseCommands(rdd, tempGraph,count)
      }
      //println(status(mainGraph))
      println("End...")
      //status(mainGraph)
      println(graphEqual(mainGraph,secondGraph))
      status(mainGraph)
      saveGraph() // used to save graph, currently off whilst testing, willl probably set some boolean or summin
      println(count + "Complete")
      countComplete = countComplete +1;
      println()
    })
  }

  def parseCommands(rdd: RDD[String], tempGraph: Graph[VertexId, String],count:Int): Graph[VertexId, String] = {
    val startTime = System.currentTimeMillis
    var fileList = splitRDD(rdd, count)
    val endTime = System.currentTimeMillis - startTime
    println("split time = "+endTime)
    val startTime2 = System.currentTimeMillis

    var altGraph: Graph[VertexId, String] = Graph(tempGraph.vertices, tempGraph.edges, 0)
    
    //
    for(i <- 0 to (fileList.size()-1) by 1){
      val tuple = fileList.get(i)
      System.out.println("ID =" + tuple._5+ " Batch ="+tuple._6)
      var addEdgeSet = tuple._1
      var rmvEdgeSet = tuple._2
      var addNodeSet = tuple._3
      var rmvNodeSet = tuple._4
      altGraph = graphRemove(altGraph,rmvEdgeSet,rmvNodeSet)
      altGraph = graphAdd(altGraph,addEdgeSet,addNodeSet)
      altGraph = partition(altGraph)
      writeOut(addEdgeSet,addNodeSet,rmvEdgeSet,rmvNodeSet,count,i)
    }
    
    val endTime2 = System.currentTimeMillis - startTime2
    println("build time = "+endTime2)
    altGraph
  }

  def partition(graph: Graph[VertexId,String])={
    graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut).groupEdges((a, b) => {
        if(a==b){a}
        else a + " " + b
      })
  }

  def splitRDD(rdd: RDD[String],count: Int):docList = {
    var addEdgeSet = new HashSet[String]() // as we will not care about order once we are finished
    var addNodeSet = new HashSet[String]()
    var rmvEdgeSet = new HashSet[String]() // sets are used so we don't have to check if something is contained
    var rmvNodeSet = new HashSet[String]()
    var id = 0;
    var batch = 0;
    var fileList = new docList()
    var rddArray = rdd.collect()
    val commandLength = rddArray.length
    for (i <- 0 to (commandLength - 1) by 1) { // go backwards so files read first are split first (We begin at length -2 as last pos is always END OF FILE )
      var split = rddArray(i).split(" ")
      var command = split(0)
      //------------------Check if Add Edge command ------------------//
      if (command == "addEdge") {
        addEdgeSet.add(rddArray(i))
      }
      //------------------Check if Add Node command ------------------//
      else if (command == "addNode") {
        addNodeSet.add(rddArray(i))
      }
      //------------------Check if Remove edge command ------------------//
      else if (command == "rmvEdge" ) {
        rmvEdgeSet.add(rddArray(i))
      }

      //------------------Check if Remove node command ------------------//
      else if (command == "rmvNode") {
        rmvNodeSet.add(rddArray(i)) //again as set no need to check if contains
      }

      //------------------Check if end of file ------------------//
      else if (command =="FILE_INFO"){
        println(rddArray(i))
        id = split(1).toInt
        batch = split(2).toInt
      }
      //------------------Check if end of file ------------------//
      else if (command == "END_OF_FILE"){
        fileList.add((addEdgeSet,rmvEdgeSet,addNodeSet,rmvNodeSet,id,batch)) // add everything from file into tuple4 and reset sets
        addEdgeSet = new HashSet[String]() // as we will not care about order once we are finished
        addNodeSet = new HashSet[String]()
        rmvEdgeSet = new HashSet[String]() // sets are used so we don't have to check if something is contained
        rmvNodeSet = new HashSet[String]()
        id =0;
        batch = 0;
        println("END OF FILE")
      }

    }
    //println("Ending that loop fam")
    //fileList.add((addEdgeSet,rmvEdgeSet,addNodeSet,rmvNodeSet)) // last one (or if there is only one file) will not have end of file entry so must be down after loop
    sortFileList(fileList) //return the list
  }

  def sortFileList (fileList:docList): docList = {
    var size = fileList.size
    var head = subList(fileList,0,(size/2))
    var tail = subList(fileList,(size/2),size)
    if(fileList.size==0){
      //println("ZERO")
      fileList
    }
    else if(fileList.size==1){
      //println("ONE")
      fileList
    }
    else{
      var size = fileList.size
      //head = sortFileList(fileList.subList(0,(size/2)).asInstanceOf[docList])
      //tail = sortFileList(fileList.subList((size/2),size).asInstanceOf[docList])
      //head = subList(fileList,0,(size/2))
      //tail = subList(fileList,(size/2),size)
      head = sortFileList(head)
      tail = sortFileList(tail)
      //println("SPLIT")
    }
    combineFileLists(head,tail)
  }
  def subList (fileList:docList,start:Int,end:Int):docList = {
    var sublist = new docList()
    for (i <- start to (end-1) by 1) {
      sublist.add(fileList.get(i))
    }
    sublist

  }

  def combineFileLists(head:docList , tail:docList ): docList ={

    var fileList = new docList()
    var check = true
    while (check) {
      if (head.size==0){
        fileList.addAll(tail)
        check=false
      }
      else if (tail.size==0){
        fileList.addAll(head)
        check = false
      }
      else {
        var headID = head.get(0)._5
        var headBatch = head.get(0)._6
        var tailID = tail.get(0)._5
        var tailBatch = tail.get(0)._6

        if (headBatch==tailBatch) {
          if (headID < tailID) {
            fileList.add(head.get(0))
            head.remove(0)
          } else { 
            fileList.add(tail.get(0))
            tail.remove(0)
          }
        }
        else if (headBatch < tailBatch){
          fileList.add(head.get(0))
          head.remove(0)
        }
        else {
          fileList.add(tail.get(0))
            tail.remove(0)
        }
      }
    }
    fileList
  }

  def graphRemove(graph: Graph[VertexId,String], rmvEdgeSet: HashSet[String],rmvNodeSet: HashSet[String]):Graph[VertexId,String]={

    var tempGraph = graph.subgraph(epred => edgeChecker(epred,rmvEdgeSet))
    tempGraph = tempGraph.subgraph(vpred = (vid, attr) => nodeChecker((vid, attr) ,rmvNodeSet))
    tempGraph
  }

  def edgeChecker (edgeTriplet: EdgeTriplet[VertexId,String], rmvSet: HashSet[String] ):Boolean = {
    rmvSet.foreach(edge => {
      val commandSplit = edge.split(" ")
      if((edgeTriplet.srcId == (commandSplit(1).toLong)) && (edgeTriplet.dstId == (commandSplit(3).toLong)) && (edgeTriplet.attr == (commandSplit(2)))){
        //println("Removing "+edgeTriplet+" as " + edge )
        return false

      }
    })
    true
  }

  def nodeChecker(vertex : (VertexId,VertexId), rmvSet: HashSet[String]): Boolean = {
    rmvSet.foreach(vert => {
      val commandSplit = vert.split(" ")
      if (vertex._1 == commandSplit(1).toLong) {
        //println("Removing " + vertex + " as " + vert)
        return false
      }
    })
    true
  }

  def graphAdd(graph: Graph[VertexId,String], addEdgeSet: HashSet[String],addNodeSet: HashSet[String]):Graph[VertexId,String]={

    var edgeArray: Array[Edge[String]] = addEdgeSet.toArray.map(edge => {
      var command = edge.split(" ")
      Edge(command(1).toLong, command(3).toLong, command(2))
    })
    var vertArray = addNodeSet.toArray.map(vert => {
      var command = vert.split(" ")
      (command(1).toLong,1L)
    })
    var vertGraph: Graph[VertexId, String] = Graph(sc.parallelize(vertArray), sc.parallelize(Array[Edge[String]]()))
    //println("Vert Graph")
    //vertGraph.vertices.foreach(vert => println(vert._1))
    var edgeGraph: Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(edgeArray), 1L)
    //println("Edge Graph")
    //edgeGraph.triplets.foreach(edge => println(edge.srcId+" "+edge.attr+" "+edge.dstId))
    var midPointGraph = Graph(edgeGraph.vertices.union(vertGraph.vertices),edgeGraph.edges,1L)
    //println("midPoint Graph")
    //midPointGraph.triplets.foreach(edge => println(edge.srcId+" "+edge.attr+" "+edge.dstId))
    Graph(graph.vertices.union(midPointGraph.vertices),graph.edges.union(midPointGraph.edges))
  }

  def saveGraph(){
    // Write out edges and vertices of graph into file named after current time 
    val timestamp: Long = System.currentTimeMillis //get current time

    mainGraph.vertices.saveAsTextFile("prev/" + timestamp + "/vertices")

    mainGraph.edges.saveAsTextFile("prev/" + timestamp + "/edges")
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

    Graph(vertRDD, edgeRDD, 0L) // return new graphs consisting of read in vertices and edges
  }

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

  def graphEqual(graph1: Graph[VertexId, String],graph2: Graph[VertexId, String]):Boolean = {

    var temp :  RDD[EdgeTriplet[VertexId,String]] = graph1.triplets.subtract(graph2.triplets)
    if(!(temp.isEmpty)){false}
    temp = graph2.triplets.subtract(graph1.triplets)
    if(!(temp.isEmpty)){false}
    if(!((graph1.vertices.subtract(graph2.vertices)).isEmpty)){false}
    if(!((graph2.vertices.subtract(graph1.vertices)).isEmpty)){false}
    else true
  } 

  def writeOut (addEdgeSet: HashSet[String],addNodeSet: HashSet[String],rmvEdgeSet: HashSet[String],rmvNodeSet: HashSet[String],count: Int,i: Int) {
    val pw = new PrintWriter(new File("Output/output"+count+" "+i+".txt"))
    addEdgeSet.toArray.map(edge => {
      pw.write(edge+"\n")
    })
    addNodeSet.toArray.map(edge => {
      pw.write(edge+"\n")
    })  
    rmvEdgeSet.toArray.map(edge => {
      pw.write(edge+"\n")
    })  
    rmvNodeSet.toArray.map(edge => {
      pw.write(edge+"\n")
    }) 
    pw.close   
  }

  def loadStartState():Graph[VertexId, String]={
    var empty = true;
    if(empty){
      Graph.fromEdges(sc.parallelize(Array[Edge[String]]()), 0L).partitionBy(PartitionStrategy.EdgePartition2D)
    }
    else{
      readGraph("1455464045188")
    }

  }

  def status(graph: Graph[VertexId, String]) {

    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)
//    graph.triplets.map(triplet =>
//      triplet.srcId + " is the " +
//        triplet.attr + " of " +
//        triplet.dstId).foreach(println(_))
    println("vertex total: " + graph.numVertices.toInt)
//    graph.vertices.foreach(id => {
//      println(id)
//   })
  }
}
