package com.josh.mapred.basic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;


public class Order {

	public Order(String[] args) throws Exception{
		
		Configuration conf=new Configuration();
		Job job=new Job(conf, "Order");
		job.setJarByClass(Order.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 1 : 0);
		
	}

	public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			IntWritable num = new IntWritable();
			while(itr.hasMoreTokens()){
				num.set(Integer.parseInt(itr.nextToken()));
				context.write(num,  new IntWritable(0));
			}
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		public static IntWritable counter=new IntWritable(1); 
		public void reduce(IntWritable key, Iterable values, Context context) throws IOException, InterruptedException{
			context.write(counter, key);
			counter.set(counter.get()+1);
		}
	}
	
}
