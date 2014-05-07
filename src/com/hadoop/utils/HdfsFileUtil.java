package com.hadoop.utils;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS文件操作类
 * @author moonights
 *
 */
public class HdfsFileUtil {
	public static void main(String[] args) throws Exception {
		getDirectoryFromHdfs("hdfs://hadoop.master:9000/");
		//makeDir("/temp");
//		deleteHdfsFile("hdfs://hadoop.master:9000/20131129/a.txt");
		//uploadToHdfs("/usr/local/hadoop-1.0.0/README.txt", "hdfs://hadoop.master:9000/20131129/a.txt");
//		readFromHdfs("hdfs://hadoop.master:9000/20131129/a.txt","/home/hadoop/tmp/README.txt");
	}
	
	/***
	 * 创建目录路径 
	 * @param dir
	 * @throws IOException
	 */
	public static void makeDir(String dir) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path src = new Path(dir);
		fileSystem.mkdirs(src);
		fileSystem.close();
	}

	/****
	 * HDFS删除文件 
	 * @param path
	 * @throws IOException
	 */
	public static void deleteHdfsFile(String path) throws IOException {
		Configuration conf = new Configuration();
		Path delefPath = new Path(path);
		FileSystem hdfs = delefPath.getFileSystem(conf);
		if (hdfs.exists(delefPath)) {
			hdfs.delete(delefPath, true);
		} else {
			System.out.println("文件不存在：删除失败");
		}
	}

	/*****
	 * HDFS上传文件 <put>
	 * @param local
	 * @param hdfs
	 * @throws IOException
	 */
	public static void uploadToHdfs(String local, String hdfs) throws IOException {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(hdfs), config);
		FileInputStream fis = new FileInputStream(new File(local));
		OutputStream os = fs.create(new Path(hdfs));
		IOUtils.copyBytes(fis, os, 4096, true);
		os.close();
		fis.close();
	}

	/****
	 * HDFS读取文件 
	 * @param fileName
	 * @param dest
	 * @throws IOException
	 */
	public static void readFromHdfs(String fileName, String dest) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(fileName));
		OutputStream out = new FileOutputStream(dest);
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);
		while (-1 != readLen) {
			out.write(ioBuffer, 0, readLen);
			System.out.println(new String(ioBuffer));
			readLen = hdfsInStream.read(ioBuffer);
		}
		out.close();
		hdfsInStream.close();
		fs.close();
	}
	
	
	/***
	 * HDFS 获取目录 <ls>
	 * @param path
	 * @throws IOException
	 */
	public static void getDirectoryFromHdfs(String path) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(path), conf);
		FileStatus fileList[] = fs.listStatus(new Path(path));
		int size = fileList.length;
		for (int i = 0; i < size; i++) {
			if (fileList[i].isDir() == false) {
				System.out.println("filename:"
						+ fileList[i].getPath().getName() + "\tsize:"
						+ fileList[i].getLen());
			} else {
				String newpath = fileList[i].getPath().toString();
				getDirectoryFromHdfs(newpath);
			}
		}
		fs.close();
	}
}
