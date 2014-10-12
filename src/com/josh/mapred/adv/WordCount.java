package com.josh.mapred.adv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== WordCount ===");
		
		String input;
		String output;
		if(args.length==0){
			input = "/tmp/test.txt";
			output = "/tmp/wordCount";
		}
		else{
			input = args[0];
			output = args[1];
		}
		Configuration conf = new Configuration();
		
		conf.set("fs.default.name", "hdfs://192.168.31.200:8020");
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("Delete %s\n", output);
		}
		
		Job job = new Job(conf, "WordCount");
		
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		System.out.println("=== done ===");
		
	}
	
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String txt=value.toString();
			String[] words = txt.split(" ");
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

