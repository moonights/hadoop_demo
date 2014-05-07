package com.hadoop.data;

import java.io.FileWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;

import com.hadoop.utils.HdfsFileUtil;
/**
 * 
 * @author moonights
 *
 */
public class DataGenerator {
	
	public static void newFile(String content,String path) throws IOException{
		FileWriter outFile = new FileWriter(path);
		PrintWriter out = new PrintWriter(outFile);
		out.println(content);
		out.close();
		outFile.close();
	}

	
	public static void main(String[] args) throws Exception {
		String path="/home/hadoop/tmp/file1.txt";
		String content="56\n516\n312\n245\n323\n748";
		String hdfsPath="hdfs://hadoop.master:9000/demo/sort/input/file2.txt";
		DataGenerator.newFile(content, path);
		//创建完成后 上传 文件 到HDFS
//		HdfsFileUtil.makeDir("demo/sort/input");
		HdfsFileUtil.uploadToHdfs(path, hdfsPath);
	}
}
