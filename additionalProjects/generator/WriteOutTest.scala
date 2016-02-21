import scala.util.Random
import java.io._
object WriteOutTest{
	
	def main(args: Array[String]){
		val random: Random = new Random()
		val genDir: File = new File("generatedData")

		var filecount =genDir.listFiles().length;
		var pw = new PrintWriter(new File("generatedData/inputset"+filecount.toString+".txt"))
		var count =0;
		//for(a <- 1 to 100){
		while(true){
			val gen = genData()
			pw.write(gen)
			//println(gen)
			if(count == 50){
				println("inside")
				count = 0
				filecount = filecount +1
				pw.close
				Thread sleep 50000
				pw = new PrintWriter(new File("generatedData/inputset"+filecount.toString+".txt"))
				
			}
			count = count +1
		}
		pw.close
	}
	def genData():String={
		val random: Random = new Random()
		val probability: Double = random.nextDouble()
		if(probability<=0.40)
			"addEdge "+random.nextInt(5000) + " dasd " + random.nextInt(5000)+ "\n"
		else if(probability>0.40 && probability <= 0.7)
			"addNode "+random.nextInt(5000) +"\n"
		else if(probability>0.70 && probability <= 0.9)
			"rmvEdge "+random.nextInt(5000) + " dasd " + random.nextInt(5000)+ "\n"
		else 
			"rmvNode "+random.nextInt(5000) +"\n"
	}
}
