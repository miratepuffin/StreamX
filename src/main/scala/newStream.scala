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

object newStream {
  // define the type used to store the list of documents that come in
  type docList = Tuple4[HashSet[String],HashSet[String],HashSet[String],HashSet[String]] 

  val sparkConf = new SparkConf().setAppName("New Stream")
  val sc = new SparkContext(sparkConf)
  var count = 0

  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

	var mainGraph: Graph[VertexId, String] = loadStartState(1)
  val secondGraph: Graph[VertexId,String] = readGraph("secondCheck/gen")
  var lastIteration = 0L;

  def main(args: Array[String]) {
    while(true){
      if(System.currentTimeMillis > lastIteration + 5000){
        lastIteration = System.currentTimeMillis
        checkNewFiles()
      }
    }
  }

  def checkNewFiles(){
    count = count + 1
    println("Iteration " + count)
    println("Count Start: "+mainGraph.edges.count())
    println("Count Start: "+mainGraph.vertices.count())
    //sc.wholeTextFiles("hdfs://moonshot-ha-nameservice/user/bas30/output/")
    val batchFolder: RDD[(String,String)] = sc.wholeTextFiles("additionalProjects/storm-starter/output")
    val sortedBatch = batchFolder.sortBy(x=> (x._1.substring(x._1.lastIndexOf('/')+1).toInt),true,1)
    sortedBatch.foreach(pair => {
      println("processing "+pair._1 )
      if(pair._2.length < 1){
        println("Empty")
      }
      else{
        mainGraph = parseCommands(pair._2, mainGraph,secondGraph,count)
      }
      //hdfsRemoveFile(pair._1)
      localRemoveFile(pair._1)
    })
    println(count + "Complete")
    println() 
	}
		
  def parseCommands(commands: String, tempGraph: Graph[VertexId, String],secondGraph: Graph[VertexId, String],count:Int): Graph[VertexId, String]={
    //build and sort fileList
    val startTime = System.currentTimeMillis
    var fileList = splitRDD(commands, count)
    println("split time = "+(System.currentTimeMillis - startTime))
    //build new graph from filelist and old graph
		val startTime2 = System.currentTimeMillis
    val edgeVertPair = build((tempGraph.edges,tempGraph.vertices),fileList)
		var altGraph: Graph[VertexId, String] = Graph(edgeVertPair._2, edgeVertPair._1, 1L)
    println("build time = "+(System.currentTimeMillis - startTime2))
    
		//status(altGraph)
	  println(graphEqual(altGraph,secondGraph))
	  saveGraph(altGraph) // used to save graph, currently off whilst testing, willl probably set some boolean or summin
		println("Count End: "+altGraph.edges.count())
    println("Count End: "+altGraph.vertices.count())
		altGraph
  }

	def build(edgeVertPair:(RDD[Edge[String]],RDD[(VertexId,VertexId)]), fileList: docList): (RDD[Edge[String]],RDD[(VertexId,VertexId)])={     
			var addEdgeSet = fileList._1
      var rmvEdgeSet = fileList._2
      var addNodeSet = fileList._3
      var rmvNodeSet = fileList._4
			graphAdd(graphRemove(edgeVertPair,rmvEdgeSet,rmvNodeSet),addEdgeSet,addNodeSet)
	}
  def graphRemove(edgeVertPair: (RDD[Edge[String]],RDD[(VertexId,VertexId)]), rmvEdgeSet: HashSet[String],rmvNodeSet: HashSet[String]):(RDD[Edge[String]],RDD[(VertexId,VertexId)])={
		val edges = edgeVertPair._1.filter(edge => (!rmvEdgeSet.contains("rmvEdge "+edge.srcId + edge.attr + edge.dstId))&&
                                               (!rmvNodeSet.contains("rmvNode "+edge.srcId))&&
                                               (!rmvNodeSet.contains("rmvNode "+edge.dstId)))
		val verts = edgeVertPair._2.filter(vert => !rmvNodeSet.contains("rmvNode "+vert._1))
		(edges,verts)
	}
  def graphAdd(edgeVertPair: (RDD[Edge[String]],RDD[(VertexId,VertexId)]), addEdgeSet: HashSet[String],addNodeSet: HashSet[String]):(RDD[Edge[String]],RDD[(VertexId,VertexId)])={
    var edgeArray: Array[Edge[String]] = addEdgeSet.toArray.map(edge => {
      var command = edge.split(" ")
      Edge(command(1).toLong, command(3).toLong, command(2))
    })
    var vertArray = addNodeSet.toArray.map(vert => (vert.split(" ")(1).toLong,1L))

		val verticies = sc.parallelize(vertArray)
    val edges = sc.parallelize(edgeArray)
    (edgeVertPair._1.union(edges),edgeVertPair._2.union(verticies))
  }

  def splitRDD(commands: String, count: Int):docList = {
    var addEdgeSet = new HashSet[String]() // as we will not care about order once we are finished
    var addNodeSet = new HashSet[String]()
    var rmvEdgeSet = new HashSet[String]() // sets are used so we don't have to check if something is contained
    var rmvNodeSet = new HashSet[String]()
    val rddArray = commands.split("\n")
    val commandLength = rddArray.length
    for (i <- 0 until commandLength) { // go backwards so files read first are split first (We begin at length -2 as last pos is always END OF FILE )
      var split = rddArray(i).split(" ")
      var command = split(0)

      if (command == "addEdge") {addEdgeSet.add(rddArray(i))}
      else if (command == "addNode") {addNodeSet.add(rddArray(i))}
      else if (command == "rmvEdge" ) {rmvEdgeSet.add(rddArray(i))}
      else if (command == "rmvNode") {rmvNodeSet.add(rddArray(i))}
    }
    (addEdgeSet,rmvEdgeSet,addNodeSet,rmvNodeSet) // add everything from file into tuple4 and reset sets
  }

  def hdfsRemoveFile(path:String){
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(path), true)
    } catch{ case e: Exception =>
      println("can't delete" + e)
    }
  }

  def localRemoveFile(path:String){
    println("Removing")
    val file = new File(path.replace("file:",""))
    //FileUtils.deleteQuietly(file)
    FileUtils.forceDelete(file)
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
  def graphEqual(graph1: Graph[VertexId,String], graph2: Graph[VertexId,String]): Boolean = {
    var thisV = graph1.vertices
    var thisE = graph1.edges

    var otherV = graph2.vertices
    var otherE = graph2.edges
    println(thisV.count == (thisV.intersection(otherV).count))
    println(thisE.count == (thisE.intersection(otherE).count))
    thisV.count == (thisV.intersection(otherV).count) && thisE.count == (thisE.intersection(otherE).count)
  } 
  def loadStartState(batch: Int):Graph[VertexId, String]={
    Graph.fromEdges(sc.parallelize(Array[Edge[String]]()), 1L).partitionBy(PartitionStrategy.EdgePartition2D)
  }
  def cacheGraph(graph: Graph[VertexId, String], batch: Int) {
    graph.vertices.setName("vert "+batch).cache()
    graph.edges.setName("edges "+batch).cache()
  } 
  def status(graph: Graph[VertexId, String]) {
    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)
    println("vertex total: " + graph.numVertices.toInt)
  }
}
