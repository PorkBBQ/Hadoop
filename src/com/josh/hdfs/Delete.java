package com.josh.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Delete {
	public static void main(String[] args) throws IOException{
		
		String fileName = "/tmp/test.txt";
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf\\core-site.xml"));
		System.out.println(conf.get("fs.defaultFS"));
		System.setProperty("HADOOP_USER_NAME","root");
		FileSystem fs = FileSystem.get(conf);
		System.out.println(fs.delete(new Path(fileName), false)); 
		fs.close();
	}
}
