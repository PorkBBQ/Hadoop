package com.josh.hdfs;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class Cat {

	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}
	
	public static void main(String[] args) throws MalformedURLException, IOException {
		InputStream in=null;
		String fileName="hdfs://192.168.31.200:8020/tmp/test.txt";
		try{
			in = new URL(fileName).openStream();
			IOUtils.copyBytes(in, System.out, 4096, false);
		}
		finally{
			IOUtils.closeStream(in);
		}
	}
}
