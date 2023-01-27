package com.chandra.otari.wordcount.test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import com.chandra.otari.wordcount.reduce.WordCountReducer;

public class WordCountReducerTest {
	
	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		ReduceDriver<Text, IntWritable, Text, IntWritable> driver = new ReduceDriver<>();
		driver.withReducer(new WordCountReducer());
		driver.addInput(new Text("Hello"), Arrays.asList(new IntWritable(1), new IntWritable(1)));
		driver.addOutput(new Text("Hello"), new IntWritable(2));
		driver.runTest();
	}
}
