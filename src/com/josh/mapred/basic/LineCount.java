package com.josh.mapred.basic;


import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LineCount {
	
	public static void main(String[] args) throws Exception{
 
		long StartTime = System.currentTimeMillis();
		System.out.print("=== go! ===\n\n");
		run(args);
		double timeSpent=(System.currentTimeMillis() - StartTime)/1000.0;
		System.out.printf("\n=== done! ( %.2f s)\n", timeSpent);
	}
	
	public static void run(String[] args)  throws Exception{
		
		
		String input="hdfs:///tmp/test.txt";
		String output="hdfs:///tmp/linecount";
		
		System.setProperty("HADOOP_USER_NAME","root");
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.31.200:8020");
	    //conf.set("mapreduce.framework.name", "yarn");  
	    //conf.set("yarn.resourcemanager.address", "192.168.31.200:8032");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.delete(new Path(output), true))
			System.out.printf("DELETE: %s\n", output);
		
		fs.close();
		
		Job job=new Job(conf, "LineCount");

		job.setJarByClass(LineCount.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		if(job.waitForCompletion(true)){
			System.out.printf("--> %s\n", output);	
		}

	}
	
	public static class Map extends Mapper<Object, Text, Text, IntWritable>{
		private final static Text countAll=new Text("COUNT(*)");
		private final static IntWritable one=new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			context.write(countAll,  one);
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

		private IntWritable result=new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;
			for (IntWritable val:values){
				sum+=val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
}
