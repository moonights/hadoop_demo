package com.hadoop.example.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**创建目录**/
public class MakeDir {
	public static void main(String[] args) throws IOException {

		String dir = "/user/moonights/wordcount/input" ;//路径方式1
		
		Configuration conf = new Configuration();
//		conf.addResource("hdfs://192.168.200.106:9000");
		FileSystem fileSystem = FileSystem.get(conf);
		Path src = new Path(dir);
		fileSystem.mkdirs(src);
		fileSystem.close();
	}
}