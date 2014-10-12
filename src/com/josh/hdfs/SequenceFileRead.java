package com.josh.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileRead {
	public static void main(String[] args) throws IOException{
		String fsName = "hdfs://10.24.100.136:9000";
		String pathName = "/tmp/seqDemo";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fsName), conf);
		Path path = new Path(pathName);
		
		SequenceFile.Reader reader = null;
		try{
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while(reader.next(key, value)){
				String syncSeen = reader.syncSeen()? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		}
		finally{
			IOUtils.closeStream(reader);
		}
		
	}
}
