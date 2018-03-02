package lab5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer's input are local top N from all mappers.
 *  We have a single reducer, which creates the final top N.
 *
 * @author Mahmoud Parsian
 *
 */
public class TopNReducer extends Reducer<NullWritable, Text, IntWritable, Text> {

   private int N;
   private Configuration configuration;
   private SortedMap<Integer, String> top;
   private SortedMap<String, String> movie;

   public Configuration getConf() {

		return this.configuration;
	}

   @Override
   public void reduce(NullWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

      for (Text value : values) {

         String myValString = value.toString().trim();
         String[] tokens = myValString.split(",");
         String id = tokens[0];
         int rating = (int) Double.parseDouble(tokens[1]);

         top.put(rating, id);

         // keep only top N
         if (top.size() > N) {
            top.remove(top.firstKey());
         }
      }

      // emit final top N
        List<Integer> keys = new ArrayList<Integer>(top.keySet());
        for(int i = keys.size() - 1; i >= 0; i--){

        	int finalRating = keys.get(i);
        	String finalTitle = movie.get(top.get(finalRating));
        	//System.out.println(finalTitle);
         context.write(new IntWritable(finalRating), new Text(finalTitle));
      }
   }

   @Override
   protected void setup(Context context)
      throws IOException, InterruptedException {

      this.N = context.getConfiguration().getInt("N", 5);
      this.top = new TreeMap<Integer, String>();
      this.movie = new TreeMap<String, String>();

      File movieList = new File("movie_titles.txt");

	   try{

			BufferedReader br = new BufferedReader(new FileReader(movieList));
			String line;

			while((line = br.readLine()) != null){

				String[] temp = line.trim().split(",");
				movie.put(temp[0], temp[2]);
			}

			br.close();
		}
		catch(Exception e){

			e.printStackTrace();
		}
   }


}
