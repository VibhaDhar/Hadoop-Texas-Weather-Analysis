package hadoop_project;
 
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 

public class Reducer1<KEY> extends Reducer<KEY, LongWritable,KEY,LongWritable> 
{

	private LongWritable result = new LongWritable();
	
	public void reduce(KEY key, Iterable<LongWritable> values,
	Context context) throws IOException, InterruptedException
	{
		long sum = 0;
		long count =0;
		for (LongWritable val : values) {
		//sum = ((sum*count) + val.get())/(count);
		sum += val.get();
		count++;
	}
	long answer = sum/count;
	result.set(answer);
	context.write(key, result);
}

}