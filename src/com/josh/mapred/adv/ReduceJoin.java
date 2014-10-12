
package com.josh.mapred.adv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoin {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		
		Path inputPath1;
		Path inputPath2;
		Path outputPath;
		
		if(args.length==0){
			inputPath1=new Path("/user/biadmin/data/mydata");
			inputPath2=new Path("/user/biadmin/data/mydata_ref");
			outputPath=new Path("/tmp/reduceJoin");
		}
		else{
			inputPath1=new Path(args[0]);
			inputPath2=new Path(args[1]);
			outputPath=new Path(args[2]);
		}
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
			System.out.printf("Delete %s\n", outputPath.getName());
		}
		
		Job job = new Job(conf);

		job.setJarByClass(ReduceJoin.class);
		
//		job.setMapperClass(ReduceJoinMapper01.class);
		MultipleInputs.addInputPath(job, inputPath1, TextInputFormat.class, ReduceJoinMapper01.class);
		MultipleInputs.addInputPath(job, inputPath2, TextInputFormat.class, ReduceJoinMapper02.class);
		job.setReducerClass(ReduceJoinReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
//		FileInputFormat.addInputPath(job, inputPath1);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
		
		System.out.println("=== done ===");
	}
	
	public static class ReduceJoinMapper01 extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String valueStr = value.toString();
			int splitOffset = valueStr.indexOf(',');
			
			String joinKey = valueStr.substring(0, splitOffset);
			String joinValue = valueStr.substring(splitOffset+1, valueStr.length());
//			System.out.printf("%s|%d|%s|%s\n", valueStr, splitOffset, joinKey, joinValue);
			context.write(new Text(joinKey), new Text("a::"+joinValue));
		}
	}

	public static class ReduceJoinMapper02 extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String valueStr = value.toString();
			int splitOffset = valueStr.indexOf(',');
			
			String joinKey = valueStr.substring(0, splitOffset);
			String joinValue = valueStr.substring(splitOffset+1, valueStr.length()-splitOffset+1);
//			System.out.printf("%s|%d|%s|%s\n", valueStr, splitOffset, joinKey, joinValue);
			context.write(new Text(joinKey), new Text("b::"+joinValue));
		}
	}	
	
	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			List<String> listA = new ArrayList<String>();
			List<String> listB = new ArrayList<String>();
			for(Text value:values){
				String valueStr = value.toString();
				if(valueStr.substring(0,3).equals("a::")){
					listA.add(valueStr.substring(3));
				}
				else{
					listB.add(valueStr.substring(3));
				}
			}
			
			for(Object a:listA.toArray()){
				for(Object b:listB.toArray()){
//					System.out.println(a.toString());
					context.write(key, new Text(a+","+b));
				}
			}
		}
	}
	
}
