import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Reducer;

public static class TrafficOutputReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
	
	public static int globalMax = Integer.MIN_VALUE;
	public static StringBuilder sensIDMax = new StringBuilder();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{

		for(IntWritable value : values)
		{
			if(globalMax < value.get())
			{
				globalMax = value.get();
				sensIDMax.setLength(0);	
				sensIDMax.insert(0, key.toString());
				
			}
			
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		context.write(new Text(sensIDMax.toString()), new IntWritable(globalMax));
		
	}
}