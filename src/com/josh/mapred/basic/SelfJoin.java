package com.josh.mapred.basic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


public class SelfJoin {
	
	public SelfJoin(String[] args) throws Exception{
		
		Configuration conf=new Configuration();
		
		Job job=new Job(conf, "SelfJoin");
		job.setJarByClass(SelfJoin.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 1 : 0);
		
	}

	public static class Map extends Mapper<Object, Text, Text, Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String seprator="\t";
			ArrayList<String> values=split(value.toString(), seprator);
			context.write(new Text(values.get(0)), new Text("1\t"+values.get(1)));
			context.write(new Text(values.get(1)), new Text("2\t"+values.get(0)));
		}
	}


	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			ArrayList<String> grandfas=new ArrayList<String>();
			ArrayList<String> grandsons=new ArrayList<String>();
			
			for (Text val1:values){
				ArrayList<String> relation=split(val1.toString(), "\t");
				if(relation.get(0).equals("1")){
					grandfas.add(relation.get(1));
				}
				if(relation.get(0).equals("2"))
					grandsons.add(relation.get(1));
			}
			
			for(String grandfa:grandfas){
				for(String grandson:grandsons){
					context.write(new Text(grandson), new Text(key.toString().concat("\t").concat(grandfa)));
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
