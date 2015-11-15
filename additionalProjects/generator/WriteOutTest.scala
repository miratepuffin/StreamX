import scala.util.Random
import java.io._
object writeOutTest{
	
	def main(args: Array[String]){
		val random: Random = new Random()
		var filecount =0;
		var pw = new PrintWriter(new File("generatedData/inputset"+filecount.toString+".txt"))
		var count =0;
		//for(a <- 1 to 100){
		while(true){
			val gen = genData()
			pw.write(gen)
			println(gen)
			if(count == 2){
				println("inside")
				count = 0
				filecount = filecount +1
				pw.close
				pw = new PrintWriter(new File("generatedData/inputset"+filecount.toString+".txt"))
				Thread sleep 5000
			}
			count = count +1
		}
		pw.close
	}
	def genData():String={
		val random: Random = new Random()
		val probability: Double = random.nextDouble()
		if(probability<=0.40)
			"addEdge "+random.nextInt(10) + " dasd " + random.nextInt(10)+ "\n"
		else if(probability>0.40 && probability <= 0.6)
			"addNode "+random.nextInt(10) +"\n"
		else if(probability>0.60 && probability <= 0.8)
			"rmvEdge "+random.nextInt(10) + " dasd " + random.nextInt(10)+ "\n"
		else 
			"rmvNode "+random.nextInt(10) +"\n"  
	}
}