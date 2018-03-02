package lab5;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TopNDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int exitcode = ToolRunner.run(new Configuration(),
				new TopNDriver(), args);
		System.exit(exitcode);
	}

	public int run(String[] args) throws Exception {
		

		if (args.length != 3) {
			System.out.printf("Usage: " + this.getClass().getName() +  " <N> <input dir> <output dir>\n");
			System.exit(-1);
		}
		

		Job job = new Job(getConf());
		job.setJarByClass(TopNDriver.class);
		
		int N = Integer.parseInt(args[0]);
		
	    job.getConfiguration().setInt("N", N);

		job.setJobName("TopNDriver");

	
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
	    job.setNumReduceTasks(1);

	    job.setMapOutputKeyClass(NullWritable.class);   
	    job.setMapOutputValueClass(Text.class);
	    		
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
