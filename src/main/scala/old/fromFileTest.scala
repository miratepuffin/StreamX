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
import org.apache.spark.streaming.dstream.DStream
import java.util.Date
import java.io.File;

object fromFileTest {
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
    if (args.length < 1) {
      System.err.println("Requires text file location")
      System.exit(1)
    } //requires you to specify a IP/port from which the data is incoming (localhost 9999 for example)

    val lines = ssc.textFileStream(args(0))
    
    extractRDD(lines)//extract the RDD inside the dstream

    ssc.start() //run the stream and await termination
    ssc.awaitTermination()
  }

  def extractRDD ( lines: DStream[String] ) {
    lines.foreachRDD( rdd => { // allow for access to the RDD inside of the DStream
      count=count+1 // count of itterations run
      if (!rdd.partitions.isEmpty){ //check if the partition is empty (no new data) as otherwise we get empty collection exception
        rdd.foreach {line => println(line)
        actionCheck(line.split(" "))} //otherwise we check what actions have been requested
      }
      doStuff() // random method for print statements etc
      //saveGraph() // used to save graph, currently off whilst testing, willl probably set some boolean or summin
    })
  }

  def actionCheck(fields: Array[String]){
    if(fields(0)=="addEdge"){ //check if given command is add and passes edge to be added to graph
      addEdge(Edge(fields(1).toLong, fields(3).toLong, fields(2)))
    }
    else if (fields(0)=="addNode"){ // checks if add node and passes ID
      addNode(fields(1))
    }
    else if (fields(0)=="rmvEdge"){// checks if remove edge and passes all fields to be utilised in subgraph check
      removeEdge(fields)
    }
    else if (fields(0)=="rmvNode"){//checks if remove node and passes ID
      removeNode(fields(1))
    }
    else if(fields(0)=="loadOld"){ // checks if loading old graphs and passes date/time
     myGraph = readGraph(timeChecker(Array(fields(1),fields(2))))
    }
  }


  def addEdge(newedge: Edge[String]){
    val newgraph : Graph[VertexId, String] = Graph.fromEdges(sc.parallelize(Array(newedge)), 1L) // create a new graph with given edge
    myGraph = Graph(myGraph.vertices.union(newgraph.vertices), myGraph.edges.union(newgraph.edges),0) // and unionise that with previous graph
    
  }
  def addNode(newNode: String){
    val id: Array[(VertexId,VertexId)] = Array((newNode.toLong,1))  
    val idRDD: RDD[(VertexId,VertexId)] = sc.parallelize(id) //create vertex RDD from ID and union with graph
    myGraph = Graph(myGraph.vertices.union(idRDD), myGraph.edges,0)
  }

  def removeEdge(fields: Array[String]){ //create subgraph based on given fields
    myGraph = myGraph.subgraph(epred => checkEdge(fields,epred)) 
  }
  def checkEdge(fields: Array[String],epred:EdgeTriplet[VertexId,String]):Boolean={
    if((epred.srcId != (fields(1).toLong)) || (epred.dstId != (fields(3).toLong)) || (epred.attr != (fields(2)))) true
    else false
  }

  def removeNode(nodeName: String){// create subgraph where all ID's do not equal given id
    myGraph = myGraph.subgraph(vpred = (vid, attr) => vid != nodeName.toLong ) 
  }

  def saveGraph(){ // write out edges and vertices of graph into filed named after current time
    val timestamp: Long = System.currentTimeMillis//get current time
    myGraph.vertices.saveAsTextFile("prev/"+timestamp+"/vertices") // save vertices
    myGraph.edges.saveAsTextFile("prev/"+timestamp+"/edges") //save edges
  }
  def readGraph(savePoint:String):Graph[VertexId,String]={//return graph for given time
    val vertx = sc.textFile("prev/"+savePoint+"/vertices") // read in vertices for given time
    val vertRDD: RDD[(VertexId,VertexId)] = vertx.map(line=>{ //for each line
          val split = line.split(",") // split the serialized data
          (split(0).substring(1).toLong,1L) // and turn back into a Vertex
      })
    val edges = sc.textFile("prev/"+savePoint+"/edges") // read in edges
    val edgeRDD: RDD[Edge[String]] = edges.map(line => { // for each line
         val split = line.split(",") // split serialized data
         val src = split(0).substring(5).toLong // extract the src node ID
         val dest = split(1).toLong // destination node Id
         val edg = split(2).substring(0,split(2).length()-1) // and edge information
         Edge(src,dest,edg) // create new edge with extracted info
      })
    Graph(vertRDD,edgeRDD,0L) // return new graphs consisting of read in vertices and edges
    }

  def timeChecker(givenTime:Array[String]):String={ // returns time of graph closest to given time
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss") // set format of time
    val date = format.parse(givenTime(0)+" "+givenTime(1)).getTime() // turn given time into Unix long
    val previous = new File("prev") // get the main directory for saving graphs as file
    val prevList = previous.listFiles.toList // extract list of all graphs
    var difference = Long.MaxValue // set starting values for the difference as Maximum 
    var chosen:String = ""  // and chosen graph as empty string
    prevList.foreach(file=> { //for each graph
      val name = file.getName //get the name (unix time created)
      try{
        val fileTime = name.toLong //turn the name back into a unix time 
        val diff = Math.abs(date-fileTime) // find the difference to the given time
        // uf the difference is less than current record, set this graph as chosen
        if(diff<difference){ 
          difference = diff 
          chosen=name 
        }
      }catch{
        case e: NumberFormatException => {}
      }
    })
    chosen
  }

  def doStuff(){
    println("Count: "+ count)
    myGraph.triplets.map(triplet => triplet.srcId + " is the " + triplet.attr + " of " + triplet.dstId).collect.foreach(println(_))
    myGraph.vertices.foreach(id => {println(id)})
  }
}