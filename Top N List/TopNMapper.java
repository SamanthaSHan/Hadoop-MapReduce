package lab5;


import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;


public class TopNMapper extends
   Mapper<Object, Text, NullWritable, Text> {

   private int N;
   private SortedMap<Double, String> top;

   @Override
   public void map(Object key, Text value, Context context)
         throws IOException, InterruptedException {

      String line = value.toString();
      String[] tokens = line.split("\t");
      String movieID = tokens[0];
      Double rating = Double.parseDouble(tokens[1]);
      
      String pair = movieID + "," + rating;
            
      top.put(rating, pair);
      // keep only top N
      if (top.size() > N) {
         top.remove(top.firstKey());
      }
      
   }
   
   @Override
   protected void setup(Context context) throws IOException,
         InterruptedException {
      this.N = context.getConfiguration().getInt("N", 5); // default is top 5
      this.top = new TreeMap<Double, String>();
   }
   

   @Override
   protected void cleanup(Context context) throws IOException,
         InterruptedException {
      for (String str : top.values()) {
         context.write(NullWritable.get(), new Text(str));
      }
   }

}