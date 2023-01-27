package com.chandra.otari.maxtemp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		String[] data = line.split(" ");
		
		context.write(new IntWritable(Integer.valueOf(data[0])), new IntWritable(Integer.valueOf(data[1])));
	}
	
}
