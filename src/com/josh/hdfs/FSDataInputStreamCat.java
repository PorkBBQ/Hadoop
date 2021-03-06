package com.josh.hdfs;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FSDataInputStreamCat {

	public static void main(String[] args) throws IOException {
		String uri = "hdfs://10.24.100.136:9000/data/mydata";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FSDataInputStream in = null;
		try{
			in = fs.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			
			System.out.print("\n\n");
			in.seek(100);
			IOUtils.copyBytes(in, System.out, 4096, false);

		}
		finally{
			IOUtils.closeStream(in);
		}
	}
}
