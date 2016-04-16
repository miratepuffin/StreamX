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
			if(count == 100000){
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
		val idPool = 5000
		if(probability<=0.30)
			"addEdge "+random.nextInt(idPool) + " dasd " + random.nextInt(idPool)+ "\n"
		else if(probability>0.3 && probability <= 0.5)
			"addNode "+random.nextInt(idPool) +"\n"
		else if(probability>0.5 && probability <= 0.6)
			"rmvEdge "+random.nextInt(idPool) + " dasd " + random.nextInt(idPool)+ "\n"
		else 
			"rmvNode "+random.nextInt(idPool) +"\n"
	}
}
