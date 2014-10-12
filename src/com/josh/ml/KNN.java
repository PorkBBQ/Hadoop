
package com.josh.ml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KNN {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		String input;
		String output;
		
		if(args.length!=2){
			input = "/data/knn";
			output = "/tmp/ml/knn";
		}
		else{
			input = args[0];
			output = args[1];
		}
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		conf.set("inX", "1.0,1.0");
		conf.set("k", "3");
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("Delete %s\n", output);
		}
		
		Job job = new Job(conf, "KNN");
		
		job.setJarByClass(KNN.class);
		
		job.setMapperClass(KNNMapper.class);
		job.setReducerClass(KNNReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);

		System.out.println("=== done ===");
	}
	
	public static class KNNMapper extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] values = value.toString().split(",", 2);
			String inX = context.getConfiguration().get("inX");
//			context.write(new Text(values[0]), new Text(values[1]));
			context.write(new Text(""), new Text(""+callDistance(values[1], inX)+" "+values[0]));
		}
		private double callDistance(String v1, String v2){
			String[] v1s = v1.split(",");
			String[] v2s = v2.split(",");
			double dist_sq_sum=0.0;
			for(int i=0;i<v1s.length;i++){
				double diff_sq = Double.parseDouble(v1s[i])-Double.parseDouble(v2s[i]);
				dist_sq_sum+=Math.pow(diff_sq, 2);
			}
			
			return Math.pow(dist_sq_sum, 0.5);
		}
	}
	
	public static class KNNReducer extends Reducer<Text, Text, Text, DoubleWritable>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			List<String> dists = new ArrayList<String>();
			
			for(Text value:values){
				dists.add(value.toString());
			}
			Collections.sort(dists);
			int k=Integer.parseInt(context.getConfiguration().get("k"));
			for(int i=0;i<k;i++){
				String[] temp=dists.get(i).split(" ");
				context.write(new Text(temp[1]), new DoubleWritable(Double.parseDouble(temp[0])));
			}
		}
	}
}
