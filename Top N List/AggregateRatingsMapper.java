package lab5;

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

	    String line = value.toString().trim();
	    String[] tokens = line.split(",");
	    
	    if(tokens.length != 3){
	    	
	    	return;
	    }
	    
	    int myKey = Integer.parseInt(tokens[0]);
	    double myVal = Double.parseDouble(tokens[2]);
	    
	    context.write(new IntWritable(myKey), new DoubleWritable(myVal));
	    
	  }
}
