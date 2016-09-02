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