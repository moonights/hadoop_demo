package com.hadoop.examples.ship;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.examples.WordCount.IntSumReducer;
import com.hadoop.examples.ship.ShipCounter.ShipnameMapper;
import com.hadoop.examples.ship.ShipCounter.ShipnameReducer;

/**
 * 去重:注意 和count参数的不同 
 * @author moonights
 *
 */
public class ShipDup {
	public static class ShipDupMapper extends Mapper<Object, Text, Text, Text> {
		private Text shipname=new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			shipname=value;
			context.write(shipname,new Text(""));
		}
	}
	
	public static class ShipDupReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			  context.write(key, new Text(""));
		  }
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] ars = new String[] { "/ship/input", "/ship/dup_out" };
		String[] otherArgs = new GenericOptionsParser(conf, ars).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: ShipCounter <input> <output>");
			System.exit(2);
		}

		Job job = new Job(conf, "count ship names");
		job.setJarByClass(ShipDup.class);
		// set mapper class
		job.setMapperClass(ShipDupMapper.class);
		job.setCombinerClass(ShipDupReducer.class);
		// set reducer class
		job.setReducerClass(ShipDupReducer.class);
		// set output key format
		job.setOutputKeyClass(Text.class);
		// set output value format
		job.setOutputValueClass(Text.class);
		// get input file path
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// get output file path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
