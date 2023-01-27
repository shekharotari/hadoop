package com.chandra.otari.subpatent.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.chandra.otari.subpatent.map.SubPatentMapper;

public class SubPatentMapperTest {
	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		MapDriver<LongWritable, Text, IntWritable, Text> driver = new MapDriver<>();
		driver.withMapper(new SubPatentMapper());
		
		driver.addInput(new LongWritable(1), new Text("1 1.232"));
		driver.addInput(new LongWritable(2), new Text("1 1.45"));
		driver.addInput(new LongWritable(3), new Text("1 1.153"));
		driver.addInput(new LongWritable(4), new Text("2 2.179"));
		driver.addInput(new LongWritable(5), new Text("2 2.111"));
		driver.addInput(new LongWritable(6), new Text("3 3.233"));
		
		driver.addOutput(new IntWritable(1), new Text("1.232"));
		driver.addOutput(new IntWritable(1), new Text("1.45"));
		driver.addOutput(new IntWritable(1), new Text("1.153"));
		driver.addOutput(new IntWritable(2), new Text("2.179"));
		driver.addOutput(new IntWritable(2), new Text("2.111"));
		driver.addOutput(new IntWritable(3), new Text("3.233"));
		
		driver.runTest();
	}
}
