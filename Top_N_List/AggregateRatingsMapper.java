/*
 *  Mapper that processes (movie ID, User ID, rating)
 *  Input: Netflix data of user ratings for movies
 *  Output: (MovieID, Rating)
 *
 *  Written by: Hakkyung Lee and Samantha Han
 */
package top_n_list;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AggregateRatingsMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{

	@Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {

		//take one line at a time
	    String line = value.toString().trim();
	    String[] tokens = line.split(",");

	    //Checks if the data is in right format
	    if(tokens.length != 3){
	    	return;
	    }

	    //take in 0th index and 2nd index from tokens
	    int myKey = Integer.parseInt(tokens[0]); //Movie ID
	    double myVal = Double.parseDouble(tokens[2]); //Rating

	    context.write(new IntWritable(myKey), new DoubleWritable(myVal));

	  }
}
