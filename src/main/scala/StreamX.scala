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
	// map += {"rmvE"  -> rmvEdge}
	// map += {"rmvN"  -> rmvNode}
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

				graph = graphUnion(graph, graphRecursion(spout))
			}

			printGraph
		})
	}

	def graphRecursion(spout : List[String]) : Graph[String, String] = spout match{
		case x :: tail => {
			var data = x.split(" ")

			// Get function from Map acc. to Key, 
			// .get to unwrap from Some <function>
			var operation = map.get(data.head).get

			// Return graph object as product of operation
			var subgraph = operation(data.tail)

			graphUnion(graphRecursion(tail), subgraph)
		}
		case _ => Graph(users, edges)
	}

	// Constucts a new graph from Edges
	def addEdge(data : Array[String]) : Graph[String, String] = {
		var src = data(0).toLong
		var dst = data(1).toLong
		var msg = data(2)

		var edge = sc.parallelize(Array(Edge(src, dst, msg)))
		Graph.fromEdges(edge, "John Doe")
	}

	// Adds a new Node to a graph
	def addNode(data : Array[String]) : Graph[String, String] = {
		var id = data(0).toLong
		var name = data(1)

		var user = sc.parallelize(Array((id, name)))
		Graph(user, edges) // edges is default empty
	}

	// Takes 2 graph obj. and returns the union
	def graphUnion(
		graph1 : Graph[String, String], 
		graph2 : Graph[String, String]
	) : Graph[String, String] = {

		Graph(
			graph1.vertices.union(graph2.vertices),
			graph1.edges.union(graph2.edges)
		)
	}

	def printGraph() {
		graph.vertices.foreach(x => println(x))
	}
}