import java.io.*;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class FirstWord
{
	public static class FirstWordMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String words[] = value.toString().split(" ");
			context.write(new Text(words[0]),new IntWritable(1));
		}
	}
	
	public static class FirstWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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
		Job job = Job.getInstance(conf,"First Word Count");
		
		job.setJarByClass(FirstWord.class);
		job.setMapperClass(FirstWordMapper.class);
		job.setReducerClass(FirstWordReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
