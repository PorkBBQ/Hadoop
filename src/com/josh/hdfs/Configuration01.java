package com.josh.hdfs;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Configuration01 {
	public static void main(String[] args){
		Configuration conf = new Configuration();
		conf.addResource(new Path("conf\\conf_1.xml"));
		conf.addResource(new Path("conf\\core-site.xml"));
		
		System.out.println(conf.get("color"));
		System.out.println(conf.get("size-weight"));
		System.out.println(conf.get("noSuchProperty"));
		System.out.println(conf.get("fs.defaultFS"));
		for(Entry<String, String> property:conf){
			System.out.println(property);
		}
	}
}

