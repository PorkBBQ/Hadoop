package com.josh.mapred.adv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class DataFrameWritable extends HashMap implements Writable{

	private Map<String ,Series> dataFrame;
	
	public class Series extends ArrayList{
		public Series(){
			super();
		}
		public Series(ArrayList values){
			super();
			for(Object value:values){
				this.add(value);
			}
		}
	}
	
	public void setDateFrame(ArrayList<Series> columns){
		setSeries(columns);
	}
	
	public void setSeries(ArrayList<Series> series){
		for(int i=0;i<series.size();i++){
			this.put(""+i, series.get(i));
		}
	}
	
	public void setSeries(ArrayList<Series> series, ArrayList<String> columns){
		if(series.size()==columns.size()){
			for(int i=0;i<series.size();i++){
				this.put(columns.get(i), series.get(i));
			}
		}
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
	}
	
}
