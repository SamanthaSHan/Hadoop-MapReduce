/*
 *This is the driver for Job 2 of Top-N-List
 *Mapper: TopNMapper
 *Reducer: TopReducer
 *
 *Written by: Hakkyung Lee and Samantha Han
 */
package top_n_list;

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
				new TopNDriver(), args); //Use ToolRunner for job execution
		System.exit(exitcode);
	}

	public int run(String[] args) throws Exception {
		//take in 3 arguments: topN input output
		if (args.length != 3) {
			System.out.printf("Usage: " + this.getClass().getName() +  " jar <jarfile.jar> lab5.Driver -files <path/to/movie_titles.txt> <N> <input dir> <output dir>\n");
			System.exit(-1);
		}

		Job job = new Job(getConf());
		job.setJarByClass(TopNDriver.class);

		//parse top N as integer
		int N = Integer.parseInt(args[0]);

	    job.getConfiguration().setInt("N", N);

		job.setJobName("TopNDriver");


		FileInputFormat.setInputPaths(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
	    job.setNumReduceTasks(1); //set to one reduce task

	    //Mapper output (_,(movieID, rating))
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Text.class);

	    //Reducer output (movieID, movie title)
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
