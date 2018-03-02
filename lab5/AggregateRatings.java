package lab5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AggregateRatings extends Configured implements Tool {

  public static void main(String[] args) throws Exception {

    
	int exitcode = ToolRunner.run(new Configuration(), new AggregateRatings(), args);
	System.exit(exitcode);
  }
  
  public int run(String[] args) throws Exception{
	  
    if (args.length != 2) {
      System.out.printf(
          "Usage: WordCount <input dir> <output dir>\n");
      System.exit(-1);
    }

    Job job = new Job(getConf());
    
    job.setJarByClass(AggregateRatings.class);
    
    job.setJobName("AggregateRatingJob1");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(AggregateRatingsMapper.class);
    job.setReducerClass(SumReducer.class);
    
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
        
   
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}
