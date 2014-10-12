
package com.josh.ml;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NaiveBayes_WordSet {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		System.out.println("=== go ===");

		String input = "/data/bayes";
		String output = "/tmp/ml/bayes";
		if(args.length==2){
			input=args[0];
			output=args[1];
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
		Job job = new Job(conf, "cute, innocent, and kawaii NaiveBayes_WordSet");
		job.setJarByClass(NaiveBayes_WordSet.class);
		
		job.setMapperClass(NaiveBayes_WordSetMapper.class);
		job.setCombinerClass(NaiveBayes_WordSetReducer.class);
		job.setReducerClass(NaiveBayes_WordSetReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		
		System.out.println("=== done ===");

	}
	
	public static class NaiveBayes_WordSetMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] words = value.toString().split(" ");
			for(String word:words){
				context.write(new Text(word), NullWritable.get());
			}
		}
	}
	
	public static class NaiveBayes_WordSetReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
}
