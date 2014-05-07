package com.hadoop.examples;

import java.io.IOException;

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
import org.apache.hadoop.util.GenericOptionsParser;

import com.hadoop.utils.HdfsFileUtil;

/**
 * 
 * @author moonights
 *
 */
public class Sort {
	
	//map将输入中的value化成IntWritable类型，作为输出的key
    public static class Map extends  Mapper<Object,Text,IntWritable,IntWritable>{ 

        private static IntWritable data=new IntWritable(); 

        //实现map函数
        public void map(Object key,Text value,Context context) throws IOException,InterruptedException{ 
            String line=value.toString(); 
            System.out.println("line="+line);
            if(!line.equals("")){
            	data.set(Integer.parseInt(line)); 
                context.write(data, new IntWritable(1)); 
            }
        } 

    } 
    
    //reduce将输入中的key复制到输出数据的key上，
    //然后根据输入的value-list中元素的个数决定key的输出次数
    //用全局linenum来代表key的位次
    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{ 

        private static IntWritable linenum = new IntWritable(1); 
        
        //实现reduce函数
        public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{ 
            for(IntWritable val:values){ 
                context.write(linenum, key); 
                linenum = new IntWritable(linenum.get()+1); 
            } 
        } 

    } 


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
//		conf.set("mapred.job.tracker", "hadoop.master:9001");
		String[] ars = new String[] { "/demo/sort/input", "/demo/sort/output" };
		String[] otherArgs = new GenericOptionsParser(conf, ars).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			System.err.println("Usage: sorter <input> <output>");
			System.exit(2);
		}
		HdfsFileUtil.deleteHdfsFile(ars[1]);
		Job job = new Job(conf, "sort ......");
		job.setJarByClass(Sort.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// get output file path
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
