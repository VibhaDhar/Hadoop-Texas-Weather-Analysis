package hadoop_project;
 
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
public class Mapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {
 
  private LongWritable count = new LongWritable();
 
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("\n");
    Text datakey = new Text();
    LongWritable datavalue = new LongWritable();
    int i = 0;
   
    
    try {
		for(i=0;i<split.length;i++)
		{
				StringTokenizer tokens = new StringTokenizer(split[i]);
		    	datakey.set(tokens.nextToken());
		    	tokens.nextToken();
		    	tokens.nextToken();
		    	tokens.nextToken();
		    	datavalue.set(Long.parseLong(tokens.nextToken()));

		    	context.write(datakey,datavalue);
		    }
	} catch (NumberFormatException e) {
		
		e.printStackTrace();
	}
    	
   
    
  }
}