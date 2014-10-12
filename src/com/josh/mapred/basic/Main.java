package com.josh.mapred.basic;

import java.io.File;
import java.util.ArrayList;
import java.util.*;

 
public class Main {

	public static void main(String[] args)throws Exception {
		Calendar cal = Calendar.getInstance();
		Date time1 = cal.getTime();

		String action="GroupBy";
		String inputPath="hdfs://192.168.0.110:9000/biginsights/hive/warehouse/josh.db/t1";
		String outputPath="data/Output";
		String[] paths={inputPath, outputPath};
		removeDir(paths[1]);
		
		if(action=="WordCount")
			new WordCount(paths);
		
		if(action=="Distinct")
			new Distinct(paths);
		
		if(action=="Order")
			new Order(paths);
		
		if(action=="SelfJoin")
			new SelfJoin(paths);
		
		if(action=="OuterJoin")
			new OuterJoin(paths);
		
		if(action=="Count")
			new Count(paths);
		
		if(action=="GroupBy")
			new GroupBy(paths);
		
		Date time2 = cal.getTime();
		System.out.print("Time Cost: ");
		System.out.println(time2.getTime()-time1.getTime());

	}
	
	public static void removeDir(String dir){

		File file = new File(dir);

		if(!file.exists()){
			return;
		}
		
		if(file.isFile()){
			System.out.println("DELETE " + file.toString());
			file.delete();
			return;
		}
		
		if(file.isDirectory()){
			File[] subs=file.listFiles();
			for(int i=0;i<subs.length;i++){
				try{
					removeDir(subs[i].toString());
				}
				finally{}
			}
			System.out.println("DELETE " + file.toString());
			file.delete();
			return;
		}
	}
	
}
