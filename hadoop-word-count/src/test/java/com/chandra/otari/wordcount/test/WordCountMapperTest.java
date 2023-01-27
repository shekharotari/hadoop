package com.chandra.otari.wordcount.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.chandra.otari.wordcount.map.WordCountMapper;

public class WordCountMapperTest {
	
	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		Text inputData = new Text("Hello Hadoop, Hello MapReduce");
		
		MapDriver<LongWritable, Text, Text, IntWritable> driver = new MapDriver<>();
		driver.withMapper(new WordCountMapper());
		driver.addInput(new LongWritable(1), inputData);
		driver.addOutput(new Text("Hello"), new IntWritable(1));
		driver.addOutput(new Text("Hadoop"), new IntWritable(1));
		driver.addOutput(new Text("Hello"), new IntWritable(1));
		driver.addOutput(new Text("MapReduce"), new IntWritable(1));
		
		driver.runTest();
	}
}
