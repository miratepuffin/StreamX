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

object combineTest {
  // define the type used to store the list of documents that come in
  type docList = ArrayList[Tuple6[HashSet[String],HashSet[String],HashSet[String],HashSet[String],Int,Int]] 

  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(15))
  var count = 0
  var countComplete = 0
	println(sparkConf.toDebugString)
  // Turn off the 100's of messages
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

	var mainGraph: Graph[VertexId, String] = loadStartState(1)
  val secondGraph: Graph[VertexId,String] = readGraph("secondCheck/gen50")

  def main(args: Array[String]) {
    var lines: DStream[String] = ssc.textFileStream("hdfs://moonshot-ha-nameservice/user/bas30/output/") 
    extractRDD(lines) // Extract RDD inside the dstream
    // Run stream and await termination
    ssc.start()
    ssc.awaitTermination()
  }

  def extractRDD(lines: DStream[String]){
    // Put RDD in scope
	  lines.count.print
	  lines.foreachRDD(rdd => {
	  	count = count + 1
	  	println("Iteration " + count)
	  	// Check if the partition is empty (no new data)
	  	// otherwise; causes empty collection exception.
			if (!rdd.isEmpty) {
				println("Count Start: "+secondGraph.edges.count())
				mainGraph = parseCommands(rdd, mainGraph,secondGraph,count)
				println("End...")
	 		  println(count + "Complete")
	 		  countComplete = countComplete +1;
	 		  println()
			}
		})
  }

  def parseCommands(rdd: RDD[String], tempGraph: Graph[VertexId, String],secondGraph: Graph[VertexId, String],count:Int): Graph[VertexId, String]={
    //build and sort fileList
    val startTime = System.currentTimeMillis
    var fileList = splitRDD(rdd, count)
    println("split time = "+(System.currentTimeMillis - startTime))
    //build new graph from filelist and old graph
		val startTime2 = System.currentTimeMillis
    val edgeVertPair = recursiveBuild((tempGraph.edges,tempGraph.vertices),fileList)
		var altGraph: Graph[VertexId, String] = Graph(edgeVertPair._2, edgeVertPair._1, 0L)
    println("build time = "+(System.currentTimeMillis - startTime2))
    
		//status(altGraph)
	  println(graphEqual(altGraph,secondGraph))
	  //saveGraph(mainGraph) // used to save graph, currently off whilst testing, willl probably set some boolean or summin
		println("Count End: "+altGraph.edges.count())
		altGraph
  }

	def recursiveBuild(edgeVertPair:(RDD[Edge[String]],RDD[(VertexId,VertexId)]), fileList: docList): (RDD[Edge[String]],RDD[(VertexId,VertexId)])={
		if(fileList.size() <1){
			edgeVertPair
		}
		else{
      val tuple = fileList.get(0)
      println("ID =" + tuple._5+ " Batch ="+tuple._6)
			//println(fileList.size())      
			var addEdgeSet = tuple._1
      var rmvEdgeSet = tuple._2
      var addNodeSet = tuple._3
      var rmvNodeSet = tuple._4
			fileList.remove(0)
			val tempPair = graphAdd(graphRemove(edgeVertPair,rmvEdgeSet,rmvNodeSet),addEdgeSet,addNodeSet)
			//println("Count Inter: "+tempGraph.edges.count())
      recursiveBuild(tempPair,fileList)
    }
	}


  def graphRemove(edgeVertPair: (RDD[Edge[String]],RDD[(VertexId,VertexId)]), rmvEdgeSet: HashSet[String],rmvNodeSet: HashSet[String]):(RDD[Edge[String]],RDD[(VertexId,VertexId)])={
		
		val edges = edgeVertPair._1.filter(edge => !rmvEdgeSet.contains("rmvEdge "+edge.srcId + edge.attr + edge.dstId))
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




  def partition(graph: Graph[VertexId,String])={
    graph.partitionBy(PartitionStrategy.CanonicalRandomVertexCut).groupEdges((a, b) => {
        if(a==b){a}
        else a + " " + b
      })
  }

  def splitRDD(rdd: RDD[String],count: Int):docList = {
	println("inside split")
    var addEdgeSet = new HashSet[String]() // as we will not care about order once we are finished
    var addNodeSet = new HashSet[String]()
    var rmvEdgeSet = new HashSet[String]() // sets are used so we don't have to check if something is contained
    var rmvNodeSet = new HashSet[String]()
    var id = 0;
    var batch = 0;
    var fileList = new docList()
	println("trying to collect")
   var rddArray = rdd.collect()
	println("finished collecting")
		if(rddArray.length <10){

			println(rddArray(0))					
			println(rddArray(1))
			println(rddArray(2))
		}
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

    Graph(vertRDD, edgeRDD, 0L) // return new graphs consisting of read in vertices and edges
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

    thisV.count == (thisV.intersection(otherV).count) && thisE.count == (thisE.intersection(otherE).count)
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

  def loadStartState(batch: Int):Graph[VertexId, String]={
      //if(batch == 1){
        Graph.fromEdges(sc.parallelize(Array[Edge[String]]()), 0L).partitionBy(PartitionStrategy.EdgePartition2D)
      //}
      //else{
	    //  var vertNum =(-1);
      //  var edgeNum =(-1);
      //  val rdds = sc.getPersistentRDDs
      //  for((k,v) <- rdds){
			//		case v:Some[RDD[(VertexId, VertexId)]] =>{
      //    	if(v.get.name == "vert "+batch){
      //      	vertNum = k
      //    	}
			//		}
			//		case v:Some[RDD[Edge[String]]] =>{
      //  		if(v.get.name == "edges "+batch){
			//				edgeNum = k
			//			} 
			//		}
			//		case None => println("no name")
       // }
			//	 var vertex = rdds.get(vertNum) match {
			//			case v:Some[RDD[(VertexId, VertexId)]] => v.get
		//				case None => println("non")
			//	 }
			//	 var edges = rdds.get(edgeNum) match {
			//			case e:Some[RDD[Edge[String]]] => e.get
			//			case None => println("non")
			//	 }
				 //Graph(vertex, edges, 0L) 
      //  Graph.fromEdges(sc.parallelize(Array[Edge[String]]()), 0L).partitionBy(PartitionStrategy.EdgePartition2D)
    	//}	
  }

  def cacheGraph(graph: Graph[VertexId, String], batch: Int) {
      graph.vertices.setName("vert "+batch).cache()
      graph.edges.setName("edges "+batch).cache()
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
