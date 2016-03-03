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
				println("checking new files")
        checkNewFiles()
      }
    }
  }

  def checkNewFiles(){
    count = count + 1
    println("Iteration " + count)
		val startTime = System.currentTimeMillis()
    val batchFolder: RDD[(String,String)] = sc.wholeTextFiles("hdfs://moonshot-ha-nameservice/user/bas30/output/")
    //val batchFolder: RDD[(String,String)] = sc.wholeTextFiles("additionalProjects/storm-starter/output")
		println("Mapping")
		val docListRDD: RDD[(String,docList)] = batchFolder.map(pair => {
			var addEdgeSet = new HashSet[String]() // as we will not care about order once we are finished
	    var addNodeSet = new HashSet[String]()
  	  var rmvEdgeSet = new HashSet[String]() // sets are used so we don't have to check if something is contained
  	  var rmvNodeSet = new HashSet[String]()
  	  val rddArray = pair._2.split("\n")
  	  val commandLength = rddArray.length
  	  for (i <- 0 until commandLength) { // go backwards so files read first are split first (We begin at length -2 as last pos is always END OF FILE )
  	    var split = rddArray(i).split(" ")
  	    var command = split(0)
	
  	    if (command == "addEdge") {addEdgeSet.add(rddArray(i))}
  	    else if (command == "addNode") {addNodeSet.add(rddArray(i))}
  	    else if (command == "rmvEdge" ) {rmvEdgeSet.add(rddArray(i))}
  	    else if (command == "rmvNode") {rmvNodeSet.add(rddArray(i))}
  	  }
			(pair._1, (addEdgeSet,rmvEdgeSet,addNodeSet,rmvNodeSet))
    })
	
    val rdds = docListRDD.map(pair => {
			val sets = pair._2
	
			val addEdges = sets._1.map(string => {
				val split = string.split(" ")				
				Edge(split(1).toLong, split(3).toLong, split(2))
			}).toArray

			val addNodes = sets._3.map(string => {
				val id = string.split(" ")(1).toLong
				(id, 1L)
			}).toArray

			val rmvEdges = sets._2.map(string => {
				val split = string.split(" ")				
				Edge(split(1).toLong, split(3).toLong, split(2))
			})

			val rmvNodes = sets._4.map(string => {
				val id = string.split(" ")(1).toLong
				(id, 1L)
			})
					
      (pair._1, (addEdges, rmvEdges, addNodes, rmvNodes))
		})
		rdds.cache
		val beforeCollect = System.currentTimeMillis()
		println("Before collect: "+(System.currentTimeMillis() - startTime))
		val batches = rdds.sortBy(x => (x._1.substring(x._1.lastIndexOf('/')+1).toInt), true, 1).collect
		println("After collect: "+(System.currentTimeMillis() - beforeCollect))			
		val afterCollect = System.currentTimeMillis()
		batches.foreach(batch => {
			println(batch._1)
			val activities = batch._2
			mainGraph = graphRemove(mainGraph, activities._2, activities._4)
			mainGraph = graphAdd(mainGraph, activities._1, activities._3)
			hdfsRemoveFile(batch._1)
		})
		println("Finish: "+(System.currentTimeMillis() - afterCollect))
		println("Edge Count:" + mainGraph.edges.count)
		println(count + "Complete")
    println() 		
	}

  def graphRemove(graph: Graph[Long,String], rmvEdgeSet: HashSet[Edge[String]], rmvNodeSet: HashSet[(Long,Long)]): Graph[Long, String] = {
		val edges = graph.edges.filter(edge => (!rmvEdgeSet.contains(edge))&&
                                           (!rmvNodeSet.contains((edge.srcId,1L)))&&
                                           (!rmvNodeSet.contains((edge.dstId,1L))))
		val verts = graph.vertices.filter(vert => !rmvNodeSet.contains((vert._1,1L)))
		Graph(verts,edges,1L)
	}

  def graphAdd(graph: Graph[Long,String], edgeArray: Array[Edge[String]],vertArray: Array[(VertexId,Long)]):Graph[Long,String]={
		val verticies = sc.parallelize(vertArray)
    val edges = sc.parallelize(edgeArray)
    Graph(graph.vertices.union(verticies),graph.edges.union(edges),1L)
  }

  def hdfsRemoveFile(path:String){
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(path.replace("file:","")), true)
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
  def graphEqual(graph1: Graph[Long,String], graph2: Graph[Long,String]): Boolean = {
    var thisV = graph1.vertices
    var thisE = graph1.edges
    var otherV = graph2.vertices
    var otherE = graph2.edges		
    thisV.count == (thisV.intersection(otherV).count) && thisE.count == (thisE.intersection(otherE).count)
  } 
  def loadStartState(batch: Int):Graph[Long, String]={
    Graph.fromEdges(sc.parallelize(Array[Edge[String]]()), 1L).partitionBy(PartitionStrategy.EdgePartition2D)
  }
  def cacheGraph(graph: Graph[Long, String], batch: Int) {
    graph.vertices.setName("vert "+batch).cache()
    graph.edges.setName("edges "+batch).cache()
  } 
  def status(graph: Graph[VertexId, String]) {
    println("Performing batch processing...")
    println("edge total: " + graph.numEdges.toInt)
    println("vertex total: " + graph.numVertices.toInt)
  }
}
