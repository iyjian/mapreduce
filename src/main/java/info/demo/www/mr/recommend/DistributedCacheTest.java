package info.demo.www.mr.recommend;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class DistributedCacheTest {

  public static class Map
      extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputVal = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // String[] cols = value.toString().split(",");
      // outputKey.set(cols[0]);
      // outputVal.set(cols[1] + ":" + cols[2]);
      // context.write(outputKey, outputVal);

      BufferedReader bufferedReader = new BufferedReader(new FileReader("cacheSample1"));
      
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        String[] cols = line.split(",");
        outputKey.set(cols[0]);
        outputVal.set(cols[1] + ":" + cols[2]);
        context.write(outputKey, outputVal);        
      }

      bufferedReader.close();

    }
  }

  public static class Reduce
      extends Reducer<Text, Text, Text, Text> {
    
    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {

      context.write(key, key);
    
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "DistributedCacheTest");


    job.addCacheFile(new URI("/rattings_middle/ratings_middle.csv#cacheSample1"));


    job.setJarByClass(ItermOccurrence.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
