package com.chandra.otari.maxtemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
//		System.out.println("Key=" + key);
		int maxTemp = 0;
		for (IntWritable value : values) {
//			System.out.println("value=" + value);
			if (maxTemp < value.get()) {
				maxTemp = value.get();
			}
		}
		context.write(key, new IntWritable(maxTemp));
	}
	
}
