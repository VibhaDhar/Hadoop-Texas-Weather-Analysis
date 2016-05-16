/*Vibha Dhar(vxd5020)
Hadoop Project-6
Collaboration with Romanch Patel*/

/*http://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html*/
/*http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-launch-custom-jar-cli.html*/
/*http://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-plan-file-systems.html*/
/*http://www.drdobbs.com/database/hadoop-writing-and-running-your-first-pr/240153197*/
package hadoop_project;
import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
public class Execution extends Configured implements Tool {
 
  @Override
  public int run(String[] args) throws Exception
  {
    @SuppressWarnings("deprecation")
	Job job = new Job(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getSimpleName());
    
    job.setMapperClass(Mapper1.class);
    job.setReducerClass(Reducer1.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
 /*passing the command line arguments as the input and output folder path*/
    FileInputFormat.addInputPath(job, new Path(args[0]));
    
    FileSystem fs = FileSystem.get(new URI(args[1]),getConf());
    if(fs.exists(new Path(args[1])))
    {
  
       fs.delete(new Path(args[1]),true);
    }
    
    else
    {
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    }
    
    return job.waitForCompletion(true) ? 0 : 1;
  }
 
  public static void main(String[] args) throws Exception {
	  int rc = ToolRunner.run(new Execution(), args);
	  System.exit(rc);
  }
}