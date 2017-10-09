import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Pivot {

  public static class PivotMapper extends Mapper<Object, Text, IntWritable, Text>{
      @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] result = value.toString().split(",");
        for (int i = 0; i < result.length ; i++) {
            context.write(new IntWritable(i),new Text(result[i]));
        }
    }
  }

  public static class PivotReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
    private Text result = new Text();
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String t = "";
        for (Text val : values) {
            t += val.toString() + ",";
        }
        result.set(t);
        context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "pivot");
    job.setJarByClass(Pivot.class);
    job.setMapperClass(PivotMapper.class);
    job.setCombinerClass(PivotReducer.class);
    job.setReducerClass(PivotReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}