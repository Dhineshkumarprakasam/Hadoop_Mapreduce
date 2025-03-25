import java.util.*;
import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;

public class Topn
{
	public static class TopnMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String val=value.toString();
			String elements[] = val.split(",");
			
			context.write(new Text(elements[0]), new IntWritable(Integer.parseInt(elements[1])));
		}
	}
	
	public static class TopnReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public static HashMap<String,Integer> map = new HashMap<>();
	
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			String k = key.toString();
			int sum=0;
			for(IntWritable i : values)
				sum+=i.get();
			
			map.put(k,sum);
		}
		
		public static String find_top()
		{
			int max=Integer.MIN_VALUE;
			String top="";
			
			for(var i : map.entrySet())
			{
				if(i.getValue()>max)
				{
					max=i.getValue();
					top=i.getKey();
				}
			}
			
			return top;
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			for(int i=0;i<3;i++)
			{
				String top=find_top();
				context.write(new Text(top),new IntWritable(map.get(top)));
				map.remove(top);
			}	
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TopN");
		
		job.setJarByClass(Topn.class);
		job.setMapperClass(TopnMapper.class);
		job.setReducerClass(TopnReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
