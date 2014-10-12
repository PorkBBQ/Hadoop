package com.josh.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileWriteDemo {
	private static final String[] DATA={"1", "2", "3", "4"};
	
	public static void main(String[] args) throws IOException{
		String fsName="hdfs://10.24.100.136:9000";
		String pathName="/tmp/mapDemo";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fsName), conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		MapFile.Writer writer = null;
		
		try{
			writer = new MapFile.Writer(conf, fs, pathName, key.getClass(), value.getClass());
			writer.setIndexInterval(128);
			for(int i=0;i<1024;i++){
				key.set(i+1);
				value.set(DATA[i % DATA.length]);
				writer.append(key, value);
			}
		}
		finally{
			IOUtils.closeStream(writer);
		}
	}
}
