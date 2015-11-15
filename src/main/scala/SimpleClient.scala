// Simple client

import java.net._
import java.io._
import scala.io._

object simpleClient{

def main(args: Array[String]){

	val s = new Socket(InetAddress.getByName("localhost"), 9999)
	lazy val in = new BufferedSource(s.getInputStream()).getLines()
	val out = new PrintStream(s.getOutputStream())
	println("sent")
	out.println("addEdge 3 edg 1")
	out.flush()
	println("Received: " + in.next())

	s.close()
}
}