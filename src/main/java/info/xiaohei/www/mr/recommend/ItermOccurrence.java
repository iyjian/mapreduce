package info.xiaohei.www.mr.recommend;

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

public class ItermOccurrence {

  public static class Map
      extends Mapper<Object, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputVal = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] cols = value.toString().split(",");
      outputKey.set(cols[0]);
      outputVal.set(cols[1] + ":" + cols[2]);
      context.write(outputKey, outputVal);
    }
  }

  public static class Reduce
      extends Reducer<Text, Text, Text, Text> {
    
    private Text outputVal = new Text();

    public void reduce(Text key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      List<String> valueList = new ArrayList<>();
      for (Text value : values) {
        valueList.add(value.toString());
      }
      Collections.sort(valueList);
      outputVal.set(String.join(",", valueList));
      context.write(key, outputVal);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ItemOccurenceStep1");
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