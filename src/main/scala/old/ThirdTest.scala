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

object SteadyTest {

  val sparkConf = new SparkConf().setAppName("NetworkWordCount")
  val sc = new SparkContext(sparkConf)
  val ssc = new StreamingContext(sc, Seconds(10))

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: NetworkWordCount <hostname> <port>")
      System.exit(1)
    } //requires you to specify a IP/port from which the data is incoming (localhost 9999 for example)

    //create the context from which you are streaming
   
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //turn off the 100's of messages so you can see the output

    val lines = ssc.socketTextStream(args(0), args(1).toInt, StorageLevel.MEMORY_AND_DISK_SER)
    //set the input as the IP/port specified

    var relationships : RDD[Edge[String]] = sc.parallelize((Array(Edge(3L, 7L, "collab"),
                                                          Edge(5L, 3L, "advisor"), 
                                                          Edge(2L, 5L, "colleague"),
                                                          Edge(5L, 7L, "pi"))))
    var myGraph : Graph[VertexId, String] = Graph.fromEdges(relationships, 1L)
    // create some base data and then create the graph from it
    // graph can be set to null, this just allows for ease of use

    lines.foreachRDD( rdd => {
      if(!rdd.partitions.isEmpty){ //check if the partition is empty (no new data) as otherwise we get empty collection exception
        
        if  (myGraph == null){ // if the graph is null then we just set myGraph to the graph from the new data
          
          val edges: RDD[Edge[String]] = rdd.map { line =>
            
            val fields = line.split(" ")
            Edge(fields(0).toLong, fields(2).toLong, fields(1))
            // data in the form: number(id) string(connection) number(id)
          }

          myGraph = Graph.fromEdges(edges, 1L)
        }

        else {//otherwise we compute the new graph and then unionise it with the previous
          
          val newedges: RDD[Edge[String]] = rdd.map { line =>
            val fields = line.split(" ")
            Edge(fields(0).toLong, fields(2).toLong, fields(1))
          }

          val newgraph : Graph[VertexId, String] = Graph.fromEdges(newedges, 1L)
          myGraph = Graph(
            myGraph.vertices.union(newgraph.vertices),
            myGraph.edges.union(newgraph.edges),
            0
          )

          println("num edges = " + myGraph.numEdges); // print out some stuff on the graph so we know it is working 
          println("num vertices = " + myGraph.numVertices);
        }
    }
    })

    /*val edges: 
*/
    ssc.start()
    ssc.awaitTermination()
  }
}