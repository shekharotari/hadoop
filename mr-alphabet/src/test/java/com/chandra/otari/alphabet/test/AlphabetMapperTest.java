package com.chandra.otari.alphabet.test;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import com.chandra.otari.alphabet.map.AlphabetMapper;

public class AlphabetMapperTest {
	@Test
	public void processValidRecord() throws FileNotFoundException, IOException {
		Text inputData = new Text("Hello  everyone  this  is  a  sample  dataset.");
		
		MapDriver<LongWritable, Text, IntWritable, Text> driver = new MapDriver<>();
		driver.withMapper(new AlphabetMapper());
		
		driver.addInput(new LongWritable(1), inputData);
		
		driver.addOutput(new IntWritable(5), new Text("Hello"));
		driver.addOutput(new IntWritable(8), new Text("everyone"));
		driver.addOutput(new IntWritable(4), new Text("this"));
		driver.addOutput(new IntWritable(2), new Text("is"));
		driver.addOutput(new IntWritable(1), new Text("a"));
		driver.addOutput(new IntWritable(6), new Text("sample"));
		driver.addOutput(new IntWritable(7), new Text("dataset"));
		
		driver.runTest();
	}
}
