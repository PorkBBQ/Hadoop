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

public class Average{
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		
		System.out.println("=== go ===");
		
		String input = "/data/mydata";
		String output="/tmp/mrdp/Average";
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://10.24.100.136:9000");
		conf.set("mapred.reduce.tasks", "3");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(output))){
			fs.delete(new Path(output), true);
			System.out.printf("DELETE: %s\n", output);
		}
		fs.close();
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MinMaxCount");
		job.setJarByClass(Average.class);
		job.setMapperClass(AverageMapper.class);
		job.setReducerClass(AverageReducer.class);
		job.setCombinerClass(AverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AverageWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
//		KeyValueTextInputFormat.addInputPath((JobConf) conf, new Path(input));
//		CombineFileInputFormat.addInputPath(job, new Path(input));

		
		job.waitForCompletion(true);
		System.out.println("=== done ===");
	}
	
	public static class AverageWritable implements Writable {
		private double sum=0.0;
		private double average=0.0;
		private long count=1;
		 
		
		public AverageWritable(){}
		
		public AverageWritable(double sum, long count){
			this.sum=sum;
			this.count=count;
			this.average=sum/count;
		}
		
		public double getSum(){
			return this.sum;
		}
		
		public void setSum(double sum){
			this.sum=sum;
		}
		
		public long getCount(){
			return this.count;
		}
		
		public void setCount(long count){
			this.count=count;
		}

		public double getAverage(){
			return this.average;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			out.writeDouble(this.sum);
			out.writeLong(this.count);
			out.writeDouble(this.average);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException{
			this.sum=in.readDouble();
			this.count=in.readLong();
			this.average=in.readDouble();
		}
		
		public String toString(){
			return this.getSum() + "\t" + this.getCount()+ "\t" + this.getAverage();
		}
	}
	
	public static class AverageMapper extends Mapper<Object, Text, Text, AverageWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] strs = value.toString().split(",");
			context.write(new Text(strs[0]), new AverageWritable(Double.parseDouble(strs[2]), 1));
		}
	}
	
	public static class AverageReducer extends Reducer<Text, AverageWritable, Text, AverageWritable>{
		public void reduce(Text key, Iterable<AverageWritable> values, Context context) throws IOException, InterruptedException{
			double sum=0;
			long count=0;
			for(AverageWritable value:values){
				sum=sum+value.getSum();
				count+=value.getCount();
			}
			context.write(key, new AverageWritable(sum, count));
		}
	}

}
