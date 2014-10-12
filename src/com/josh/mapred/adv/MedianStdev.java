package com.josh.mapred.adv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianStdev {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		String input="/data/mydata";
		String output="/tmp/mrdp/MedianStdev";
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("DELETE %s\n", output);
		}
		
		Job job=new Job(conf);
		job.setJarByClass(MedianStdev.class);
		
		job.setMapperClass(MedianStdevMapper.class);
		job.setReducerClass(MedianStdevReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MedianStdevWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		System.out.println("=== done ===");
	}
	
	public static class MedianStdevMapper extends Mapper<Object, Text, Text, DoubleWritable>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] strs = value.toString().split(",");
			context.write(new Text(strs[0]), new DoubleWritable(Double.parseDouble(strs[2])));
		}
	}
	
	public static class MedianStdevReducer extends Reducer<Text, DoubleWritable, Text, MedianStdevWritable>{
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
			double sum=0;
			double sum2=0;
			List<Double> arr = new ArrayList<Double>();
			double median;
			double stdev;
			int length;
			for (DoubleWritable value:values){
				sum+=value.get();
				sum2+=value.get()*value.get();
				arr.add(value.get());
			}
			Collections.sort(arr);
			length=arr.size();
			if(length%2==1)
				median=arr.get((length-1)/2);
			else
				median=(arr.get(length/2)+arr.get(length/2-1))/2;
			
			stdev=Math.sqrt((sum2-sum*sum/length)/(length-1));
			context.write(key, new MedianStdevWritable(median, stdev));
		}
	}
	
	public static class MedianStdevWritable implements Writable{
		private double median=0;
		private double stdev=0;
		
		public MedianStdevWritable(double median, double stdev){
			this.median=median;
			this.stdev=stdev;
		}
		
		public void setMedian(double median){
			this.median=median;
		}
		
		public double getMedian(){
			return this.median;
		}
		
		public void setStdev(double stdev){
			this.stdev=stdev;
		}

		public double getStdev(){
			return this.stdev;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			this.median=in.readDouble();
			this.stdev=in.readDouble();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(this.median);
			out.writeDouble(this.stdev);
		}
		
		public String toString(){
			return this.median + "\t" + this.stdev;
		}
	}
}
