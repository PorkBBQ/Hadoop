package com.josh.mapred.adv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedianStdev2 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		String input="/data/mydata";
		String output="/tmp/mrdp/MedianStdev2";
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("DELETE %s\n", output);
		}
		
		Job job=new Job(conf);
		job.setJarByClass(MedianStdev2.class);
		
		job.setMapperClass(MedianStdevMapper.class);
		job.setReducerClass(MedianStdevReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(SortedMapWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MedianStdevWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		System.out.println("=== done ===");
	}
	
	public static class MedianStdevMapper extends Mapper<Object, Text, Text, SortedMapWritable>{
		private SortedMapWritable smw = new SortedMapWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] strs = value.toString().split(",");
			smw.put(new DoubleWritable(Double.parseDouble(strs[2])), new LongWritable(1));
			context.write(new Text(strs[0]), smw);
		}
	}
	
	public static class MedianStdevReducer extends Reducer<Text, SortedMapWritable, Text, MedianStdevWritable>{
		public void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException{
			double sum=0;
			double sum2=0;
			List<Double> arr = new ArrayList<Double>();
			double median;
			double stdev;
			int count=0;

			for (SortedMapWritable value:values){
				System.out.printf("%s, %s, %s\n", key, value, value.size());
				for(Entry<WritableComparable, Writable> entry:value.entrySet()){
					System.out.printf("    %s, %s, %s\n", key, entry.getKey(), entry.getValue());	
					
					double k = ((DoubleWritable)entry.getKey()).get();
					long v = ((LongWritable)entry.getValue()).get();
//					System.out.printf("%f, %d\n", k, v);
					count+=v;
					sum+=k*v;
					sum2+=k*k*v;
				}
//				System.out.printf("%f, %d\n", count);
//				System.out.println(count);
			}
//			
//			System.out.println(count);
			median=0.0;
			stdev=Math.sqrt((sum2-sum*sum/count)/(count-1));
			context.write(key, new MedianStdevWritable(median, stdev));
//			context.write(key, new MedianStdevWritable(1,2.0));
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

	public static class TreeMapWritable implements Writable{

		private TreeMap<Double, Long> treeMap = new TreeMap<Double, Long>();
		
		public TreeMapWritable(double key, long value){
			this.treeMap.put(key, value);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
		}
		
	}
	
}
