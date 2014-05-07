package com.hadoop.examples.ship;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;
/**
 * 
 * @author moonights
 *
 */
public class ShipCounter2 {
	public static void main(String args[]){
		String[] ars = new String[] { "/tmp", "/ship/output2" };
		JobClient client 	= 	new JobClient();
		JobConf	  conf 	=  	new JobConf(ShipCounter2.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);
		conf.setMapperClass(TokenCountMapper.class);
		conf.setCombinerClass(LongSumReducer.class);
		conf.setReducerClass(LongSumReducer.class);
		FileInputFormat.addInputPath(conf, new Path(ars[0]));
		FileOutputFormat.setOutputPath(conf, new Path(ars[1]));
		client.setConf(conf);
		try {
			client.runJob(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
