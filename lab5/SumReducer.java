package lab5;

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
		
		for (DoubleWritable value : values) {
		  
			rating += value.get();
		}
				
		context.write(key, new DoubleWritable(rating));
	}
}