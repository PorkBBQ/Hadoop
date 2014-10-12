package com.josh.mapred.adv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MapJoin
{
	public static class Map extends	Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private String round;		

		private Path[] localFiles;
		FileReader fr;
		BufferedReader br;
		
		public void setup(Context context) throws IOException, InterruptedException{
			localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			fr = new FileReader(localFiles[0].toString());
			br = new BufferedReader(fr);

		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
        	String fileline = br.readLine();
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens())
			{
				word.set(tokenizer.nextToken()+" FilePath:"+localFiles[0].toString()+" FileContext:"+fileline);
				word = word;
				context.write(word,one);
			}
		}
	}
	
	public static class Reduce extends
	Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable value:values)
				sum += value.get();
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		System.out.println("=== go ===");
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		DistributedCache.addCacheFile(new URI("file://D:/Josh/data/mydata_ref"), conf);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/tmp/mapJoin"), true);
		
		Job job = new Job(conf, "wordcount");

		job.setJarByClass(MapJoin.class);		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
       	job.setReducerClass(Reduce.class);
           
       	job.setInputFormatClass(TextInputFormat.class);
       	job.setOutputFormatClass(TextOutputFormat.class);
           
       	FileInputFormat.addInputPath(job, new Path("/user/biadmin/data/mydata"));
       	FileOutputFormat.setOutputPath(job, new Path("/tmp/mapJoin"));
           
       	job.waitForCompletion(true);
       		
		System.out.println("=== done ===");
	}
}