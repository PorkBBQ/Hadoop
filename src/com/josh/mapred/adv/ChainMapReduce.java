package com.josh.mapred.adv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class ChainMapReduce {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		Path inputPath;
		Path outputPath;
		if(args.length==0){
			inputPath = new Path("/user/biadmin/data/mydata");
			outputPath = new Path("/tmp/chainMapReduce");
		}
		else{
			inputPath = new Path(args[0]);
			outputPath = new Path(args[1]);
		}
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(outputPath, true);
		
		Job job=new Job(conf);
//		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
//		job.setMapperClass(Mapper01.class);
		Configuration confM1 = new Configuration(false);
		Job job01 = new Job();
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
//		FileInputFormat.addInputPath(job, inputPath);
//		KeyValueTextInputFormat.addInputPath(job, inputPath);
//		FileOutputFormat.setOutputPath(job, outputPath);
		
//		job.waitForCompletion(true);
		
		System.out.println("=== done ===");
	}
	
	public static class Mapper01 extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context ) throws IOException, InterruptedException{
			
			String firstColumn=key.toString();

			if(Integer.parseInt(firstColumn)<5){
				context.write(key, value);
			}
		}
	}
	
	public static class Mapper02 extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context ) throws IOException, InterruptedException{
			String firstColumn=key.toString();
			if(Integer.parseInt(firstColumn)<5){
				context.write(key, value);
			}
		}
	}
}


