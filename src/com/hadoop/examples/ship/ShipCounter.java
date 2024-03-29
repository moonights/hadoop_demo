package com.hadoop.examples.ship;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.examples.WordCount;
import com.hadoop.examples.WordCount.IntSumReducer;

/**
 * 
 * @author moonights
 *
 */
public class ShipCounter {

	public static class ShipnameMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		private Text shipname = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String name = value.toString();
			shipname.set(name);
			context.write(shipname, one);
		}
	}
	
	public static class ShipnameReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable total = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			total.set(sum);
			context.write(key, total);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ars = new String[] { "/ship/input", "/ship/count_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ars).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: ShipCounter <input> <output>");
			System.exit(2);
		}

		Job job = new Job(conf, "count ship names");
		job.setJarByClass(ShipCounter.class);
		// set mapper class
		job.setMapperClass(ShipnameMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		// set reducer class
		job.setReducerClass(ShipnameReducer.class);
		// set output key format
		job.setOutputKeyClass(Text.class);
		// set output value format
		job.setOutputValueClass(IntWritable.class);
		// get input file path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// get output file path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
