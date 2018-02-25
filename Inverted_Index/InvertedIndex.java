/** Driver class for Index Reducer map-reduce job
 * Authors: Samantha Han and Hakkyung Lee
 * Written for 427S Cloud Computing and Big Data
 * **/
package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: InvertedIndex <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(InvertedIndex.class);
    job.setJobName("Inverted Index");

	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	/** Use IndexMapper as mapper and IndexReducer as reducer**/
	job.setMapperClass(IndexMapper.class);
	job.setReducerClass(IndexReducer.class);

	/** Declare mapper output key and value types**/
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	/** Declare final output key and value types**/
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new InvertedIndex(), args);
    System.exit(exitCode);
  }
}
