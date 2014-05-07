package com.hadoop.example.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 
 * @author moonights
 *
 */
public class FileSystemCat {  
    public static void main(String[] args) throws Exception {  
        String uri="/input/README.txt";  
        Configuration conf=new Configuration();  
        conf.addResource("hdfs://hadoop.master:9001");
        FileSystem fs=FileSystem.get(URI.create(uri),conf);  
        InputStream in=null;  
        try {  
            in=fs.open(new Path(uri));  
            IOUtils.copyBytes(in, System.out, 4096, false);  
        }   finally {  
            IOUtils.closeStream(in);  
        }  
    }  
}  

