import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Minmax
{
    public static class MinmaxMapper extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String val = value.toString();
            String data[] = val.split(",");

            Text index=new Text(data[0]);
            for(int i=1;i<data.length;i++)
            	context.write(index,new IntWritable(Integer.parseInt(data[i])));
        }
    }

    public static class MinmaxReducer extends Reducer<Text, IntWritable, Text, Text>
    {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
        	int min=Integer.MAX_VALUE;
        	int max=Integer.MIN_VALUE;
        	int total=0;
        	int count=0;

        	for(IntWritable i : values)
        	{
        		if(i.get()<min)
        			min=i.get();
        		if(i.get()>max)
        			max=i.get();
        		total+=i.get();
        		count++;
        	}

        	float avg = total/count;
        	String ans=new String("Min : "+min+" Max : "+max+" Avg : "+avg);
        	context.write(key, new Text(ans));
        }
    }

    public static void main(String args[]) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Min Max");
        job.setJarByClass(Minmax.class);

        job.setMapperClass(MinmaxMapper.class);
        job.setReducerClass(MinmaxReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
