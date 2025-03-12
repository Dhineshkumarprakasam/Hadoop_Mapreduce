import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MatrixAdd
{
	public static class MatrixAddMap extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String val[]=value.toString().split(",");
			String k=val[1]+","+val[2];
			context.write(new Text(k),new IntWritable(Integer.parseInt(val[3])));
		}
	}
	
	public static class MatrixAddRed extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value : values)
				sum+=value.get();
			context.write(key,new IntWritable(sum));
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Matrix Addition");
		
		job.setJarByClass(MatrixAdd.class);
		job.setMapperClass(MatrixAddMap.class);
		job.setReducerClass(MatrixAddRed.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
