package com.josh.mapred.adv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MinMaxCount{
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.out.println("=== go ===");
		
		String input = "/data/mydata";
		String output="/tmp/mrdp/MinMaxCountTuple";
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
		}

		Job job = new Job(conf, "MinMaxCount");
		job.setJarByClass(MinMaxCount.class);
		job.setMapperClass(MinMaxCountMapper.class);
		job.setReducerClass(MinMaxCountReducer.class);
		job.setCombinerClass(MinMaxCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		System.out.println("=== done ===");
	}
	
	public static class MinMaxCountWritable implements Writable {
		private Double min=null;
		private Double max=null;
		private long count=1;
		
		public Double getMin(){
			return this.min;
		}
		
		public Double getMax(){
			return this.max;
		}
		
		public void setMin(double min){
			this.min=min;
		}
		
		public void setMax(double max){
			this.max=max;
		}
		
		public long getCount(){
			return this.count;
		}
		
		public void setCount(long count){
			this.count=count;
		}
	
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(this.min);
			out.writeDouble(this.max);
			out.writeLong(this.count);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			this.min=in.readDouble();
			this.max=in.readDouble();
			this.count=in.readLong();
		}
		public String toString(){
			return this.getMin().toString() + "\t" + this.getMax().toString() + "\t" + this.getCount();
		}
	}
	
	public static class MinMaxCountMapper extends Mapper<Object, Text, Text, MinMaxCountWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] strs = value.toString().split(",");
			MinMaxCountWritable mmcw = new MinMaxCountWritable(); 
			mmcw.setMin(Double.parseDouble(strs[2]));
			mmcw.setMax(Double.parseDouble(strs[2]));
			mmcw.setCount(1);
			context.write(new Text(strs[0]), mmcw);
		}
	}
	
	public static class MinMaxCountReducer extends Reducer<Text, MinMaxCountWritable, Text, MinMaxCountWritable>{
		public void reduce(Text key, Iterable<MinMaxCountWritable> values, Context context) throws IOException, InterruptedException{
			MinMaxCountWritable result = new MinMaxCountWritable();
			long sum=0;
			for(MinMaxCountWritable value:values){
				if(result.getMin()==null || result.getMin()>value.getMin())
					result.setMin(value.getMin());
				if(result.getMax()==null || result.getMax()<value.getMax())
					result.setMax(value.getMax());
				result.setCount(result.getCount()+value.getCount());
			}
			context.write(key, result);
		}
	}

}
