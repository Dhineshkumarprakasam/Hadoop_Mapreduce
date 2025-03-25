import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Vowels
{
    public static class VowelsMapper extends Mapper<LongWritable, Text, LongWritable, Text>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
 
            String data = value.toString();
            String result = data.replaceAll("[aeiouAEIOU]", "");
            context.write(key,new Text(result));
        }
    }

    public static class VowelsReducer extends Reducer<LongWritable, Text, Text, Text>
    {
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {
        	for(Text value : values)
        		context.write(value,new Text(""));
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Remove Vowels");

        job.setJarByClass(Vowels.class);

        job.setMapperClass(VowelsMapper.class);
        job.setReducerClass(VowelsReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
