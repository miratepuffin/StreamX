import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class hdfsWriter{
    public static void main (String [] args) throws Exception{
        try{
            Path pt=new Path("hdfs:/user/bas30/try.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
            // TO append data to a file, use fs.append(Path f)
            String line;
            line="Hello world!";
            System.out.println(line);
            br.write(line);
            br.close();
        }catch(Exception e){
            System.out.println("File not found");
        }
    }
}
