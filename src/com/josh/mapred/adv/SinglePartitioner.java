package com.josh.mapred.adv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SinglePartitioner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		String input = "/data/mydata";
		String output = "/tmp/mrdp/singlePartitioner";
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("Delete %s\n", output);
		}
		
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(SinglePartitioner.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setPartitionerClass(MyPartitioner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setNumReduceTasks(3);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		System.out.println("=== done ===");
		
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

	public static class MyPartitioner extends Partitioner{
		@Override
		public int getPartition(Object arg0, Object arg1, int arg2) {
			return 0;
		}
		
	}
}
