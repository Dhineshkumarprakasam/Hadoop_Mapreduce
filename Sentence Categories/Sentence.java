import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Sentence
{
    public static class SentenceMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line[] = value.toString().trim().split(" ");
            int len = line.length;

            String category;
            
            if(len<=4)
            	category="short";
            else if(len<=9)
            	category="medium";
            else
            	category="long";
            	
            
            context.write(new Text(category),new IntWritable(1));
            	
        }
    }

    public static class SentenceReducer extends Reducer<Text,IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
        
            int count=0;
            for(IntWritable i : values)
            	count++;
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Sentence Category");

        job.setJarByClass(Sentence.class);

        job.setMapperClass(SentenceMapper.class);
        job.setReducerClass(SentenceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
