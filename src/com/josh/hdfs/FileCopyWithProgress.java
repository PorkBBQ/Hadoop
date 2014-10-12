package com.josh.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress {
	public static void main(String[] args) throws IOException{
		String fileNameFrom = "D:\\Josh\\data\\mydata";
		String fileNameTo = "hdfs://10.24.100.136:9000/tmp/model_copy";
		
		InputStream in = new BufferedInputStream(new FileInputStream(fileNameFrom));
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fileNameTo), conf);
		
		OutputStream out = fs.create(new Path(fileNameTo), new Progressable(){
			@Override
			public void progress() {
				System.out.print(".");
			}
		});
		IOUtils.copyBytes(in, out, 4096, true);
	}
}
