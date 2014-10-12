package com.josh.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PutMerge {
	public static void main(String[] args) throws IOException{
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);
		
		Path inputPath;
		Path outputPath;
		if(args.length==0){
			inputPath = new Path("D:\\Josh\\Dropbox\\data");
			outputPath = new Path("/tmp/putMerge");
		}
		else{
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);			
		}

		hdfs.delete(outputPath, true);
		FileStatus[] inputFiles = local.listStatus(inputPath);
		
		FSDataOutputStream out = hdfs.create(outputPath);
		for(FileStatus inputFile:inputFiles){
			System.out.printf("%s\t%s\n", inputFile.getPath().getName(), inputFile.getPermission().toString());
			FSDataInputStream in = local.open(inputFile.getPath());
			byte[] buffer = new byte[256]; 
			int bytesRead = 0;	
			while((bytesRead=in.read(buffer))>0){
//				System.out.println(bytesRead);
				out.write(buffer, 0, bytesRead);
			}
			in.close();
		}
		out.close();
	}
}
