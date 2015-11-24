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

object rddReduceTest {
  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(5))
  var count = 0


  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Create empty graph
  var mainGraph: Graph[VertexId, String] = Graph.fromEdges(
    sc.parallelize(Array[Edge[String]]()), 0L
  )

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

  def extractRDD(lines: DStream[String]) {
    // Put RDD in scope
    lines.foreachRDD(rdd => {
      // Count of itterations run
      count = count + 1
      println("Itteration " + count)
      // Check if the partition is empty (no new data)
      // otherwise; causes empty collection exception.
      if (!rdd.isEmpty) {
        val tempGraph: Graph[VertexId, String] = Graph(mainGraph.vertices, mainGraph.edges, 0)
        mainGraph = parseCommands(rdd, tempGraph)
      }
      status(mainGraph) // method for print statements etc
      println("End...")
      //saveGraph() // used to save graph, currently off whilst testing, willl probably set some boolean or summin
    })
  }

  def parseCommands(rdd: RDD[String], tempGraph: Graph[VertexId, String]): Graph[VertexId, String] = {
    var addRmvTuple = reduceRDD(rdd)
    var addEdgeSet = addRmvTuple._1
    var rmvEdgeSet = addRmvTuple._2
    var addNodeSet = addRmvTuple._3
    var rmvNodeSet = addRmvTuple._4
    var rddArray2 = rdd.toArray()


    var altGraph: Graph[VertexId, String] = Graph(tempGraph.vertices, tempGraph.edges, 0)
    altGraph = graphRemove(altGraph,rmvEdgeSet,rmvNodeSet)
    altGraph = graphAdd(altGraph,addEdgeSet,addNodeSet)
    //run without improvments to make sure same graph
    val commandLength2 = rddArray2.length
    var altGraph2: Graph[VertexId, String] = Graph(tempGraph.vertices, tempGraph.edges, 0)
    for (i <- 0 to (commandLength2 - 1))
      altGraph2 = performOperation(rddArray2(i).split(" "), altGraph2)


    println("one")
    status(altGraph)
    println()
    println("two")
    status(altGraph2)

    altGraph
  }

  def reduceRDD(rdd: RDD[String]): (HashSet[String],HashSet[String],HashSet[String],HashSet[String]) = {
    var addEdgeSet = new HashSet[String]() // as we will not care about order once we are finished
    var addNodeSet = new HashSet[String]()
    var rmvEdgeSet = new HashSet[String]() // sets are used so we don't have to check if something is contained
    var rmvNodeSet = new HashSet[String]()
    var rddArray = rdd.collect()
    val commandLength = rddArray.length
    for (i <- (commandLength - 1) to 0 by -1) {
      println(rddArray(i))
      var split = rddArray(i).split(" ")
      var command = split(0)
      var src = split(1)
      var msg = ""
      var dest =""
      if(split.length > 2) {
       msg  = split(2)
       dest = split(3)
      }
      //------------------Check if Add Edge command happens later or is negated by a remove ------------------//
      if (command == "addEdge") {
       // println("is add edge")
        if (rmvNodeSet.contains("rmvNode " + src)) {
          // check if the src Id is removed lower down
          //println("remove src node counter")
          if (!rmvNodeSet.contains("rmvNode " + dest)) {
            // if it is then we check if the dest node is also removed, otherwise add it
            addNodeSet.add("addNode " + dest)
           // println("adding dest node")
          }
        }

        else if (rmvNodeSet.contains("rmvNode " + dest)) {
          // check if the dest Id is removed lower down
          addNodeSet.add("addNode " + src) // no need to check src rmv as we know it is not there from above
         // println("adding src node")
        }
        else if (rmvEdgeSet.contains("rmvEdge " + src + " " + msg + " " + dest)) {
          addNodeSet.add("addNode " + src) //no need to check if they are negated as it is checked above
          addNodeSet.add("addNode " + dest)
          //println("removing edge, still adding nodes")
        }
        else {
          //println("adding the edge - no contra")
          //if there are no remove nodes or edges then we can add the command to the subset
          addEdgeSet.add(rddArray(i))
        }
      }
      //------------------Check if Add Node command happens later or is negated by a remove ------------------//
      else if (command == "addNode") {
        if (!(rmvNodeSet.contains("rmvNode " + src))) {
          addNodeSet.add(rddArray(i))
          //println("adding node as no contra")
        }
      }
      //------------------Check if Remove edge command happens later or is negated by an add ------------------//
      else if (command == "rmvEdge" ) {
          //println("is rmvEdge")
          if (addEdgeSet.contains("addEdge " + src + " " + msg + " " + dest)) {
          //  println("contra add")
          } // check if it is negated
          else if (rmvNodeSet.contains("rmvNode " + src)) {
          //  println("src being removed already")
          } // check if negated by a node remove below
          else if (rmvNodeSet.contains("rmvNode " + dest)) {
          //  println("dest being removed already")
          } // check if negated by a node remove below
          else {
            rmvEdgeSet.add(rddArray(i))
         //   println("no contra - adding remove")
          }
        }

      //------------------Check if Remove node command happens later ------------------//
      else if (command == "rmvNode") {
          //rmv can't be negated as it removes all edges
          println("is rmvNode")
          rmvNodeSet.add(rddArray(i)) //again as set no need to check if contains
        }
      else println("this shit broke")
      println("End")
      println()
    }
    println("old length " + rddArray.length)
    println("add Node set " + addNodeSet.size)
    println("add edge set " + addEdgeSet.size)
    println("remove Node set " + rmvNodeSet.size)
    println("remove edge set " + rmvEdgeSet.size)
    (addEdgeSet,rmvEdgeSet,addNodeSet,rmvNodeSet)
  }


  def graphRemove(graph: Graph[VertexId,String], rmvEdgeSet: HashSet[String],rmvNodeSet: HashSet[String]):Graph[VertexId,String]={

    var tempGraph = graph.subgraph(epred => edgeChecker(epred,rmvEdgeSet))
    tempGraph = tempGraph.subgraph(vpred = (vid, attr) => nodeChecker((vid, attr) ,rmvNodeSet))
    tempGraph
  }
  def edgeChecker (edgeTriplet: EdgeTriplet[VertexId,String], rmvSet: HashSet[String] ):Boolean = {
    rmvSet.foreach(edge => {
      val commandSplit = edge.split(" ")
      if((edgeTriplet.srcId == (commandSplit(1).toLong)) && (edgeTriplet.dstId == (commandSplit(3).toLong)) || (edgeTriplet.attr == (commandSplit(2)))){
        println("Removing "+edgeTriplet+" as " + edge )
        return false

      }
    })
    true
  }
  def nodeChecker(vertex : (VertexId,VertexId), rmvSet: HashSet[String]): Boolean = {
    rmvSet.foreach(vert => {
      val commandSplit = vert.split(" ")
      if (vertex._1 == commandSplit(1).toLong) {
        println("Removing " + vertex + " as " + vert)
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
    var edgeGraph: Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(edgeArray), 0L)
    //println("Edge Graph")
    //edgeGraph.triplets.foreach(edge => println(edge.srcId+" "+edge.attr+" "+edge.dstId))
    var midPointGraph = Graph(edgeGraph.vertices.union(vertGraph.vertices),edgeGraph.edges,0L)
    //println("midPoint Graph")
    //midPointGraph.triplets.foreach(edge => println(edge.srcId+" "+edge.attr+" "+edge.dstId))
    Graph(graph.vertices.union(midPointGraph.vertices),graph.edges.union(midPointGraph.edges))
  }

  // Map these functions to a HashMap,
  // and use .drop(1) to pass rest of arr
  def performOperation(command: Array[String], tempGraph: Graph[VertexId, String]): Graph[VertexId, String] = {
    // Checks if add edge and passes src, dest and edge.
    if (command(0) == "addEdge") {

      addEdge(Edge(
        command(1).toLong,
        command(3).toLong,
        command(2)
      ), tempGraph)
    }
    // Checks if add node and passes ID
    else if (command(0) == "addNode") {
      addNode(command(1), tempGraph)
    }
    // Checks if remove edge, arr. utilised in subgraph check
    else if (command(0) == "rmvEdge") {
      removeEdge(command, tempGraph)
    }
    // Checks if remove node and passes ID
    else if (command(0) == "rmvNode") {
      removeNode(command(1), tempGraph)
    }
    // Checks if loading old graphs and passes date/time
    else if (command(0) == "loadOld") {
      readGraph(closestGraph(Array(command(1), command(2))))
    }
    else {
      println("reached else")
      tempGraph
    }
  }

  def addEdge(edge: Edge[String], tempGraph: Graph[VertexId, String]): Graph[VertexId, String] = {
    // Create a new graph with given edge
    val newgraph: Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(Array(edge)), 1L)
    Graph(tempGraph.vertices.union(newgraph.vertices), tempGraph.edges.union(newgraph.edges), 0)
  }

  def addNode(node: String, tempGraph: Graph[VertexId, String]): Graph[VertexId, String] = {
    // Create vertex RDD from ID and union with graph
    val idRDD: RDD[(VertexId, VertexId)] = sc.parallelize(
      Array((node.toLong, 1)): Array[(VertexId, VertexId)]
    )

    val newNodes = tempGraph.vertices.union(idRDD)

    Graph(newNodes, tempGraph.edges, 0)
  }

  def removeEdge(fields: Array[String], tempGraph: Graph[VertexId, String]): Graph[VertexId, String] = {
    //create subgraph based on given fields
    tempGraph.subgraph(epred => checkEdge(fields, epred))
  }

  def checkEdge(fields: Array[String], epred: EdgeTriplet[VertexId, String]): Boolean = {
    (epred.srcId != (fields(1).toLong)) ||
      (epred.dstId != (fields(3).toLong)) ||
      (epred.attr != (fields(2)))
  }

  def removeNode(nodeName: String, tempGraph: Graph[VertexId, String]): Graph[VertexId, String] = {
    // Create subgraph where all ID's do not equal given id
    tempGraph.subgraph(vpred = (vid, attr) => vid != nodeName.toLong)
  }

  def saveGraph() {
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

  def status(graph: Graph[VertexId, String]) {

    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)
    graph.triplets.map(triplet =>
      triplet.srcId + " is the " +
        triplet.attr + " of " +
        triplet.dstId).foreach(println(_))
    println("vertex total: " + graph.numVertices.toInt)
    graph.vertices.foreach(id => {
      println(id)
    })
  }
}
