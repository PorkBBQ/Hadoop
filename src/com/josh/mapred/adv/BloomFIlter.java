package com.josh.mapred.adv;

import org.apache.hadoop.fs.Path;

public class BloomFIlter {
	public void main(String[] args){
		System.out.println("=== go ===");
		
		String input="/data/mydata";
		String output="/tmp/mrdp/BloomFilter";
		
		Path inputPath = new Path(input);
		int numMembers=21;
		double falsePosRate=0.01;
		Path bfFile = new Path(output);
		
		int vectorSize = getOptimalBloomFilterSize(numMembers, falsePosRate);
		
		System.out.println("=== done ===");
	}
	
	public static int getOptimalBloomFilterSize(int numRecords,
			double falsePosRate) {
		int size = (int) (-numRecords * (float) Math.log(falsePosRate) / Math
				.pow(Math.log(2), 2));
		return size;
	}

	public static int getOptimalK(float numMembers, float vectorSize) {
		return (int) Math.round(vectorSize / numMembers * Math.log(2));
	}
}
