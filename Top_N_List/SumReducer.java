/*
 *  Reducer that sums the rating of all movies
 *  Input: mapper output
 *  Output: (movie id, aggregated rating)
 *
 *  Written by: Hakkyung Lee and Samantha Han (and provided by previous lab)
 */
package top_n_list;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

  @Override
	public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

	  	double rating = 0;

	  	//sums the rating for a given movie
		for (DoubleWritable value : values) {

			rating += value.get();
		}

		context.write(key, new DoubleWritable(rating));
	}
}
