import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class LogAnalysis
{
	public static class LogAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String val = value.toString();
			if(val.contains("ERROR"))
				context.write(new Text("ERROR"),value);
			else if(val.contains("WARNING"))
				context.write(new Text("WARNING"),value);
			else if(val.contains("INFO"))
				context.write(new Text("INFO"),value);
		}
	}
	
	public static class LogAnalysisReducer extends Reducer<Text, Text, Text, Text> 
	{
		int error=0;
		int warning=0;
		int info=0;
		
		public void reduce(Text key, Iterable<Text> val, Context context) throws IOException, InterruptedException
		{
			String k = key.toString();
			for(Text value : val)
			{
				if(k.equals("ERROR"))
				{
					context.write(null,value);
					error++;
				}
				else if(k.equals("WARNING"))
					warning++;
				else
					info++;
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			
			context.write(new Text("ERROR"),new Text(Integer.toString(error)));
			context.write(new Text("WARNING"),new Text(Integer.toString(warning)));
			context.write(new Text("INFO"),new Text(Integer.toString(info)));
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Log analysis");
		
		job.setJarByClass(LogAnalysis.class);
		job.setMapperClass(LogAnalysisMapper.class);
		job.setReducerClass(LogAnalysisReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
