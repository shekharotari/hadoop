package com.chandra.otari.alphabet.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.chandra.otari.alphabet.reduce.AlphabetReducer;

public class AlphabetReducerTest {

	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		ReduceDriver<IntWritable, Text, IntWritable, LongWritable> driver = new ReduceDriver<>();
		driver.withReducer(new AlphabetReducer());
		
		driver.addInput(new IntWritable(5), Arrays.asList(new Text("Hello"), new Text("World")));
		driver.addInput(new IntWritable(2), Arrays.asList(new Text("is"), new Text("am"), new Text("ah")));
		
		driver.addOutput(new IntWritable(5), new LongWritable(2));
		driver.addOutput(new IntWritable(2), new LongWritable(3));
		
		driver.runTest();
	}
	
}
