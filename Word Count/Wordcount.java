import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Wordcount {
    public static class WordMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            String words[] = line.split(" ");
            for (String i : words)
                context.write(new Text(i), new IntWritable(1));
        }
    }

    public static class WordReduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int count = 0;
            for (IntWritable i : values)
                count += i.get();
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Dhinesh kumar");
        job.setJarByClass(Wordcount.class);

        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
