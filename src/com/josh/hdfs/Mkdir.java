
package com.josh.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Mkdir {
    public static void main(String[] args) throws IOException
    {
		String fsName = "hdfs://192.168.31.200:8020";
		String dirName = "/tmp/dir1";
		mkdir(fsName, dirName);
    }
    
    public static void mkdir(String fsName, String dirName) throws IOException{
		System.out.println("\n=== mkdir ===");
	
		//System.setProperty("HADOOP_USER_NAME","root");
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fsName), conf);
        
		if(fs.mkdirs(new Path(dirName)))
			System.out.printf("\nmkdir: %s\n" , dirName);
		else
			System.out.printf("\nmkdir: %s failed.\n" , dirName);
        fs.close();
    }

}
