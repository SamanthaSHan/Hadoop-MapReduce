/** Driver class for Word Co-occurence map-reduce job
 * Authors: Samantha Han and Hakkyung Lee
 * Written for 427S Cloud Computing and Big Data
 * **/
package stubs;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class WordCo extends Configured implements Tool {

	/** Using ToolRunner to run the job**/
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new WordCo(), args);
		System.exit(exitCode);
	}
	
	@Override
	public int run(String[] args) throws Exception {

		/** If 2 arguments are not passed in, let the user know about the usage**/
		if (args.length != 2) {
			System.out.printf(
					"Usage: Word Co-occurence <input dir> <output dir>\n");
			System.exit(-1);
		}
		Job job = new Job(getConf());
		job.setJobName("Word Co-occurence");

		job.setJarByClass(WordCo.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/** Use WordCoMapper as mapper and SumReducer from wordCount as reducer**/
		job.setMapperClass(WordCoMapper.class);
		job.setReducerClass(SumReducer.class);

		/** Declare mapper output key and value types**/
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		/** Declare final output key and value types**/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean status = job.waitForCompletion(true);
		return status ? 0 : 1;
	}


}
