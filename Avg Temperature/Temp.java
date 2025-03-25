import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Temp
{
	public static class TempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String val = value.toString();
			String elements[]=val.split(",");
			
			if(elements.length==3)
				context.write(new Text(elements[0]), new FloatWritable(Float.parseFloat(elements[2])));
		}
	}
	
	public static class TempReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
		{
			float avg=0;
			for(FloatWritable i : values)
				avg+=i.get();
			
			context.write(key, new FloatWritable(avg));
		}	
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Average Temprature");
		
		job.setJarByClass(Temp.class);
		
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
} 
