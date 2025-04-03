import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Frequent {
    public static class FrequentMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String words[] = line.split(" ");
            for (String i : words)
                context.write(new Text(i), new IntWritable(1));
        }
    }

    public static class FrequentReduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
    
        static int max=Integer.MIN_VALUE;
        static String word="";
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable i : values)
                count += i.get();
            if(count>max)
            {
                max=count;
                word=key.toString();
            }
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException
        {
            context.write(new Text(word), new IntWritable(max));
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Dhinesh kumar");
        job.setJarByClass(Frequent.class);

        job.setMapperClass(FrequentMap.class);
        job.setReducerClass(FrequentReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
