package com.josh.mapred.adv;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount_Simple {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "WordCount_Simple");
		job.setJarByClass(WordCount_Simple.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://10.24.100.136:9000/user/biadmin/data/mydata"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.24.100.136:9000/tmp/wordcount_simple"));
		
		job.waitForCompletion(true);
	}
	
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String txt=value.toString();
			String[] words = txt.split(",");
			for(String word:words){
				context.write(new Text(word), new IntWritable(1));
			}
		}
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable value:values){
				sum+=value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
