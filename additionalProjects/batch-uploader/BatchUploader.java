import java.io.File;
import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
class BatchUploader{

	int batchPos;
	String folder;
	public BatchUploader(int batchPos,String folder){
		this.batchPos = batchPos;
		this.folder = folder;
	}

    public void sendToHDFS() {
        Process p;
        try {
			p = Runtime.getRuntime().exec("hadoop fs -copyFromLocal ../storm-starter/"+folder+"/" + batchPos + " /user/bas30/"+folder+"Temp");
			p.waitFor();
			p = Runtime.getRuntime().exec("hadoop fs -mv /user/bas30/"+folder+"Temp/"+batchPos+" /user/bas30/"+folder);
			p.waitFor(); 
			System.out.println("Send "+batchPos+" to hdfs");
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    public boolean uploadComplete(){
    	Process p;
        try {
			p = Runtime.getRuntime().exec("hadoop fs -ls /user/bas30/"+folder+"/"+batchPos);
			p.waitFor();
			InputStream inputStream = p.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream), 1);
            String line="";
            String templine;
            while ((templine = bufferedReader.readLine()) != null) {
                line= line +"\n"+templine;
            }
            inputStream.close();
            bufferedReader.close();
            System.out.println(line);
            if(line.contains("No such file or directory")){
            	return false;
            }
            else{
            	return true;
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
    }
    public void clearFolders(){
    	Process p;
        try {
			p = Runtime.getRuntime().exec("hadoop fs -rm -r /user/bas30/"+folder+" /user/bas30/"+folder+"Temp");
			p.waitFor();
			p = Runtime.getRuntime().exec("hadoop fs -mkdir /user/bas30/"+folder+" /user/bas30/"+folder+"Temp");
			p.waitFor();
			System.out.println("Cleared Folders");
		} 
		 catch (Exception e) {
			e.printStackTrace();
		}

    }
    public boolean checkNextFile(){
    	File nextBatch = new File("../storm-starter/"+folder+"/"+ batchPos);
    	return nextBatch.exists();
    }
    public void nextBatch(){
    	batchPos++;
    }

    public void waitforUpload(){
		System.out.println("Beginning wait");
		while(!uploadComplete()){try{Thread.sleep(100);}catch(InterruptedException e){}}
		System.out.println("File Uploaded");
    }

    public static void main(String[] args) {
    	BatchUploader bu = new BatchUploader(0,args[0]);
    	bu.clearFolders();
		while(true){
			if(bu.checkNextFile()){
				bu.sendToHDFS();
				bu.waitforUpload();
				bu.nextBatch();
			}
			else{
				try{Thread.sleep(100);}catch(InterruptedException e){}
			}
		}			
	}

}

