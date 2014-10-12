package com.josh.mapred.adv;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sample {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		String input="/data/mydata";
		String output="/tmp/mrdp/Sample";
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		conf.set("percent", "0.5");
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("DELETE %s\n", output);
		}
		
		Job job=new Job(conf);
		
		job.setJarByClass(Sample.class);
		job.setMapperClass(FilterMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		System.out.println("=== done ===");
	}
	
	public static class FilterMapper extends Mapper<Object, Text, Text, NullWritable>{
		private double percent;
		private Random rand = new Random();
		public void setup(Context context){
			percent=Double.parseDouble(context.getConfiguration().get("percent"));
		}
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			if(rand.nextDouble()<percent){
				context.write(value, NullWritable.get());
			}
		}
	}
	
}
