/** Reducer class for Index Reducer map-reduce job
 * Authors: Samantha Han and Hakkyung Lee
 * Written for 427S Cloud Computing and Big Data
 * **/
package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input: <Text,Text> where key:word, value:location (word filename@line_number)
 * Output: <Text,Text> where key:word, value:list of locations
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {

	private Text word_location_list = new Text();
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  /**Create buffer for all incoming location of a word**/
	  StringBuffer buffer = new StringBuffer();
	  
	  for(Text value: values){
		  /**If there are more than one result, split by comma**/
		  if(buffer.length() != 0){
			  buffer.append(", ");
		  }
		  buffer.append(value.toString());
	  }
	  
	  /**List of all the location a word appears**/
	  word_location_list.set(buffer.toString());
	  
	  context.write(key, word_location_list);
    
  }
}