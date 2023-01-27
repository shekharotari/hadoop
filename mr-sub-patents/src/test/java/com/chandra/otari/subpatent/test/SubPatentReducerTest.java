package com.chandra.otari.subpatent.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.chandra.otari.subpatent.reduce.SubPatentReducer;

public class SubPatentReducerTest {

	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		ReduceDriver<IntWritable, Text, IntWritable, LongWritable> driver = new ReduceDriver<>();
		driver.withReducer(new SubPatentReducer());
		
		driver.addInput(new IntWritable(1), Arrays.asList(new Text("1.232"), new Text("1.45"), new Text("1.153")));
		driver.addInput(new IntWritable(2), Arrays.asList(new Text("2.179"), new Text("2.111")));
		driver.addInput(new IntWritable(3), Arrays.asList(new Text("3.233")));
		
		driver.addOutput(new IntWritable(1), new LongWritable(3));
		driver.addOutput(new IntWritable(2), new LongWritable(2));
		driver.addOutput(new IntWritable(3), new LongWritable(1));
		
		driver.runTest();
	}
	
}
