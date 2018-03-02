/** Mapper class for Index Reducer map-reduce job
 * Authors: Samantha Han and Hakkyung Lee
 * Written for 427S Cloud Computing and Big Data
 * **/
package stubs;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input: <LongWritable,Text> where each line has line number and text, separated by tab
 * Output: <Text,Text> where key:word, value:list of locations
 */
public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text word = new Text();
	private final static Text word_location = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException,
	InterruptedException {

		/** Get file name of the document **/
		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		/**Get line using tab as separator**/
		String[] line = value.toString().split("\\t");

		if (line.length <2 ) return;

		/**Locate at which line**/
		String atLine = line[0];

		/** location of the word appears in (filename@line)**/
		word_location.set(fileName+"@"+atLine);

		/**Split the line using spaces**/
		String [] words = line[1].replaceAll("[^a-zA-Z ]", "").split("\\W+");

		/**Emit all words in that line**/
		for(int i = 0; i < words.length; i++){
			if(!words[i].isEmpty()){
				word.set(words[i].toLowerCase());
				context.write(word, word_location);
			}
		}

	}
}