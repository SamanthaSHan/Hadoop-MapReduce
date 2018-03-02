/** Mapper class for Word Co-occurence map-reduce job
 * Authors: Samantha Han and Hakkyung Lee
 * Written for 427S Cloud Computing and Big Data
 * **/
package stubs;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text pair = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		/**Trim trailing and leading spaces & split on spaces**/
		String [] words = value.toString().trim().split("\\W");
		
		/**If no words exist, move onto the next line**/
		if(words.length < 1) return;
		
		/**Loop through the words and pair them with the next word it occurs concurrently**/
		for(int i = 0; i < words.length-1; i++){
			pair.set(words[i].toLowerCase() + "," + words[i+1].toLowerCase());
			context.write(pair, new IntWritable(1));
		}

	}
}
