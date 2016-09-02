import java.io.IOException;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficMRMain {
	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Insufficient arguments. \nUsage: TrafficMRMain <input> <intermediate> <output>");
			System.exit(-1);
		}

		Job run1 = new Job();
		run1.setJarByClass(TrafficMRMain.class);
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
			run2.setJarByClass(TrafficMRMain.class);
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