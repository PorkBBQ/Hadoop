
package com.josh.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SequenceFileWrite {
	private static final String[] DATA = {"1", "2", "3", "4"};
	
	public static void main(String[] args) throws IOException{
		String fsName = "hdfs://10.24.100.136:9000";
		String uri = "/tmp/seqDemo";
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fsName), conf);
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		
		SequenceFile.Writer writer = null;
		try{
			writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
			for(int i=0;i<100;i++){
				key.set(100-i);
				value.set(DATA[i%DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
		}
		finally{
			IOUtils.closeStream(writer);
		}
		
	}

}
