
package com.josh.mapred.basic;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Count {

	public Count(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=new Job(conf, "Count");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 1 : 0);
		
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
