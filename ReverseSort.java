import java.util.*;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class ReverseSort
{
	public static class ReverseSortMap extends Mapper<LongWritable, Text, LongWritable, IntWritable> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			context.write(key, new IntWritable(Integer.parseInt(value.toString())));	
		}
	}
	
	public static class ReverseSortRed extends Reducer<LongWritable, IntWritable, NullWritable, IntWritable>
	{
		ArrayList<Integer> arr = new ArrayList<>();
		
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			for(IntWritable value : values)
				arr.add(value.get());
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException
		{
			arr.sort(Comparator.reverseOrder());
			
			for(int i=0;i<arr.size();i++)
				context.write(null,new IntWritable(arr.get(i)));
		}
	}
	
	public static void main(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Reverse Sort");
		
		job.setJarByClass(ReverseSort.class);
		job.setMapperClass(ReverseSortMap.class);
		job.setReducerClass(ReverseSortRed.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}



