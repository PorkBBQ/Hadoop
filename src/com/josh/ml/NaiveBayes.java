
package com.josh.ml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
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

public class NaiveBayes {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		System.out.println("=== go ===");

		String input = "/tmp/ml/bayes_wordSet";
		String output = "/tmp/ml/bayes";
		if(args.length==2){
			input=args[0];
			output=args[1];
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		conf.set("newText", "garbage stupid");
		conf.set("groups", "0,1,0,1,0,1");

		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}
		
		Job job = new Job(conf);
		job.setJarByClass(NaiveBayes.class);
		
		job.setMapperClass(NaiveBayesMapper.class);
//		job.setReducerClass(NaiveBayesReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.waitForCompletion(true);
		
		System.out.println("=== done ===");
	}
	
	public static class NaiveBayesMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/tmp/ml/bayes_wordSet/part-r-00000"));
			BufferedReader in = null;
			String line;
			List<String> lines = new ArrayList<String>();
			try{
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while((line=in.readLine())!=null){
					lines.add(line);
				}
			}
			finally{
				if(in!=null){
					in.close();
				}
			}
			String[] words = value.toString().split(" ");
			for(String word:words){
				context.write(new Text(word), new Text(lines.get(0)));
			}
		}
	}
	
	public static class NaiveBayesReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
}
