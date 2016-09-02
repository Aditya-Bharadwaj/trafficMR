import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficMR {

	public static int globalMax = Integer.MIN_VALUE;
	public static StringBuilder sensIDMax = new StringBuilder();
/*	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	{	
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) 
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
*/
	public static class TrafficDataMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String line = value.toString();
			String[] rows = line.split(",");    //5 - Time Stamp, 6 - Vehicle Count, 8 - Sensor ID
			String[] timestamp = rows[5].split("T");
			
			if(key.get() != 0)
			{
				int month = Integer.parseInt(timestamp[0].split("-")[1]);
				int hour = Integer.parseInt(timestamp[1].split(":")[0]); 
				if((month == 8) && (hour == 8))				
				{
					int vehicleCount = Integer.parseInt(rows[6]);
					String sensID = rows[8];
					context.write(new Text(sensID), new IntWritable(vehicleCount));			
				}
			}
		}	
	}

	public static class TrafficOutputMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String sensID = value.toString().split("	")[0];
			int vehicleCount = Integer.parseInt(value.toString().split("	")[1]);
			context.write(new Text(sensID), new IntWritable(vehicleCount));
		}
	}
	
/*
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
*/
	public static class TrafficDataReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		int vehicleSum = 0;
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) {
				vehicleSum += val.get();
			}
			
			context.write(key, new IntWritable(vehicleSum));
			vehicleSum = 0;
		}
		
	}

	public static class TrafficOutputReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
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
	
	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Insufficient arguments. \nUsage: TrafficMRMain <input> <intermediate> <output>");
			System.exit(-1);
		}

		Job run1 = new Job();
		run1.setJarByClass(TrafficMR.class);
		run1.setJobName("Traffic Flow Analysis");
		
		FileInputFormat.addInputPath(run1, new Path(args[0]));
		FileOutputFormat.setOutputPath(run1, new Path(args[1]));

		run1.setMapperClass(TrafficDataMapper.class);
		run1.setReducerClass(TrafficDataReducer.class);

		run1.setOutputKeyClass(Text.class);
		run1.setOutputValueClass(IntWritable.class);
		
		if(run1.waitForCompletion(true))
		{
			Job run2 = Job.getInstance();
			run2.setJarByClass(TrafficMR.class);
			run2.setJobName("Traffic Output Analysis");
		
			FileInputFormat.addInputPath(run2, new Path(args[1]));
			FileOutputFormat.setOutputPath(run2, new Path(args[2]));

			run2.setMapperClass(TrafficOutputMapper.class);
			run2.setReducerClass(TrafficOutputReducer.class);

			run2.setOutputKeyClass(Text.class);
			run2.setOutputValueClass(IntWritable.class);

		
			System.exit(run2.waitForCompletion(true) ? 0 : 1);
		}
	}


}

