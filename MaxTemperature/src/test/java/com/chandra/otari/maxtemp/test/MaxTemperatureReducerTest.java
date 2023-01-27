package com.chandra.otari.maxtemp.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.chandra.otari.maxtemp.MaxTemperatureReducer;

public class MaxTemperatureReducerTest {

	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable> driver = new ReduceDriver<>();
		driver.withReducer(new MaxTemperatureReducer());
		
		driver.addInput(new IntWritable(1900), Arrays.asList(new IntWritable(36), new IntWritable(29)));
		driver.addInput(new IntWritable(1901), Arrays.asList(new IntWritable(32), new IntWritable(40), new IntWritable(29)));
		
		driver.addOutput(new IntWritable(1900), new IntWritable(36));
		driver.addOutput(new IntWritable(1901), new IntWritable(40));
		
		driver.runTest();
	}
	
}
