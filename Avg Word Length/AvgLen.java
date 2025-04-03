import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AvgLen
{
    public static class AvgLenMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String words[] = value.toString().trim().split(" ");
            
            for(String word : words)
                context.write(new IntWritable(word.length()),new IntWritable(1));
        }
    }

    public static class AvgLenReducer extends Reducer<IntWritable,IntWritable, Text, FloatWritable>
    {
        static int total_words=0;
        static int total_len=0;
        
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            for(IntWritable i : values)
            {
                total_words++;
                total_len+=key.get();
            }
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            context.write(new Text("Average Length : "),new FloatWritable(total_len/total_words));
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Sentence Category");

        job.setJarByClass(AvgLen.class);

        job.setMapperClass(AvgLenMapper.class);
        job.setReducerClass(AvgLenReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
