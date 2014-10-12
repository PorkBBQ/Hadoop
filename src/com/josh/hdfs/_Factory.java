package com.josh.hdfs;

import java.io.File;
import java.io.IOException;

public class _Factory {

	public static void main(String[] args) throws IOException{
		
		String fsName = "hdfs://192.168.31.200:8020";
		String dirName = "/tmp/dir1";
		String fileName = "/tmp/test.txt";
		
		//Cat.cat(fsName, fileName);
		System.out.println("hdfs:///tmp/test.txt");
		//Mkdir.mkdir(fsName, dirName);
		
		//Delete.delete(fsName, dirName);

	}

}
