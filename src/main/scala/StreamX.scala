import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.Map
import org.apache.spark.storage.StorageLevel

class StreamX(val stream : DStream[String]) {
	// Retrieve our context(s) from the DStream
	var ssc : StreamingContext = stream.context
	var sc  : SparkContext     = ssc.sparkContext

	// Define an empty Graph object
	val users : RDD[(VertexId, String)] = sc.parallelize(Array.empty[(VertexId, String)])
	val edges : RDD[Edge[String]] = sc.parallelize(Array.empty[Edge[String]])
	var graph = Graph(users, edges)

	// Define a map of functions whose return 
	// type is a Graph object
	var map = Map[String, Array[String] => Graph[String, String]]()
	map += {"addE"  -> addEdge}
	map += {"addN"  -> addNode}
	map += {"rmvE"  -> removeEdge}
	map += {"rmvN"  -> removeNode}
	// map += {"loadG" -> loadGraph}
	// map += {"viewG" -> viewGraph}  viewG yyyy-MM-dd HH:mm:ss
	 
	// Initialise the Stream
	def init {
		readStream
	}

	// Read the data from stream
	def readStream {
		stream.foreachRDD(rdd => {
			println("Starting new batch...")
			
			if(!rdd.isEmpty) {
				var spout : List[String] = rdd.collect().toList

				graph = graphRecursion(spout)
			}

			printGraph
		})
	}

	def graphRecursion(spout : List[String]) : Graph[String, String] = spout match{
		case x :: tail => {
			var data = x.split(" ")

			// Get operation command 
			var operation = data.head
			var subgraph = Graph(users, edges)

			try {

				var function = map.get(operation).get // .get to unwrap Some<function>
				graph = function(data.tail)    // sub-, or union-, graph of function

				return graphRecursion(tail)

			} catch {
				case nse : NoSuchElementException => {
					println("The method " + operation + " isn't valid.")
				 	return graphRecursion(tail)
				}
			}	
		}

		// Edge cond. Return empty graph
		case _ => graph
	}

	// Constucts a new graph from Edges
	def addEdge(data : Array[String]) : Graph[String, String] = {
		var srcId = data(0).toLong
		var dstId = data(1).toLong
		var msg = data(2)

		var edge = sc.parallelize(Array(Edge(srcId, dstId, msg)))
		// TODO: Check for diff edges (msg included)
		//       check bi-directional
		
		var inter = edge.intersection(graph.edges)

		if(inter.isEmpty) {
			Graph(graph.vertices, edge.union(graph.edges))
		}
		else {
			graph
		}
	}

	// Adds a new Node to a graph
	def addNode(data : Array[String]) : Graph[String, String] = {
		var srcId = data(0).toLong
		var name = data(1)

		var user = sc.parallelize(Array((srcId, name)))
		Graph(user.union(graph.vertices), graph.edges)
	}

	def removeEdge(data : Array[String]) : Graph[String, String] = {
		var srcId = data(0).toLong
		var dstId = data(1).toLong

		graph.subgraph(edge => {
			(edge.srcId != srcId) || (edge.dstId != dstId)
		})
	}

	def removeNode(data : Array[String]) : Graph[String, String] = {
		var srcId = data(0).toLong

		graph.subgraph(vpred = (vId, name) => vId != srcId)
	}
	
	/* 
		HELPER METHODS
	*/

	def printGraph() {
		println("Vertices:")
		graph.vertices.foreach(x => println(x))

		println("Edges:")
		println(graph.edges.map(_.copy()).distinct.count)
		graph.edges.foreach(x => println(x))
	}
}