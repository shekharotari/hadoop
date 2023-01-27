package com.chandra.otari.maxtemp.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.chandra.otari.maxtemp.MaxTemperatureMapper;

public class MaxTemperatureMapperTest {
	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		MapDriver<LongWritable, Text, IntWritable, IntWritable> driver = new MapDriver<>();
		driver.withMapper(new MaxTemperatureMapper());
		
		driver.addInput(new LongWritable(1), new Text("1900 36"));
		driver.addInput(new LongWritable(2), new Text("1900 29"));
		driver.addInput(new LongWritable(3), new Text("1901 32"));
		driver.addInput(new LongWritable(4), new Text("1901 40"));
		driver.addInput(new LongWritable(5), new Text("1901 29"));
		
		driver.addOutput(new IntWritable(1900), new IntWritable(36));
		driver.addOutput(new IntWritable(1900), new IntWritable(29));
		driver.addOutput(new IntWritable(1901), new IntWritable(32));
		driver.addOutput(new IntWritable(1901), new IntWritable(40));
		driver.addOutput(new IntWritable(1901), new IntWritable(29));
		
		driver.runTest();
	}
}
