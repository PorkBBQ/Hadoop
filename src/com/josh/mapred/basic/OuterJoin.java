package com.josh.mapred.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


public class OuterJoin {
	
	public OuterJoin(String[] args) throws Exception{
		
		Configuration conf=new Configuration();
		
		Job job=new Job(conf, "OuterJoin");
		job.setJarByClass(OuterJoin.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for(String inputFile:split(args[0], ","))
			FileInputFormat.addInputPath(job, new Path(inputFile));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 1 : 0);
		
	}

	public static class Map extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			ArrayList<String> splitted=split(value.toString(), "\t");
			if(context.getInputSplit().toString().contains("OuterJoin_Input_1")){
				context.write( new Text(splitted.get(1)), new Text("a\t".concat(splitted.get(0))));

			}
			if(context.getInputSplit().toString().contains("OuterJoin_Input_2")){
				context.write( new Text(splitted.get(0)), new Text("b\t".concat(splitted.get(1))));
			}				
		}
	}


	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

			ArrayList<String> orgs=new ArrayList<String>();
			ArrayList<String> cities=new ArrayList<String>();
			
			for (Text val1:values){
				ArrayList<String> relation=split(val1.toString(), "\t");
				if(relation.get(0).equals("a")){
					orgs.add(relation.get(1));
				}
				if(relation.get(0).equals("b"))
					cities.add(relation.get(1));
			}
			
			for(String org:orgs){
				for(String city:cities){
					context.write(new Text(org), new Text("->  ".concat(city)));
				}
			}
	
		}
	}

	public static ArrayList<String> split(String str, String seprator){
		 
		ArrayList<String> result = new ArrayList<String>();
		int loc=str.indexOf(seprator);
		if(loc<0){
			result.add(str);
			return result;
		}
		result.add(str.substring(0, loc));
		result.addAll(split(str.substring(loc+seprator.length()), seprator));
		return result;
	}

}
