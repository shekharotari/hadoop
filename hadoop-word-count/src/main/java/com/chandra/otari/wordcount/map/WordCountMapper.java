package com.chandra.otari.wordcount.map;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// ----------------------------------------------------------------------------------------
		// Input Key - Line Number
		// Input Value - Line of text
		//
		// Extract words from line of text using regex and add a count to each word
		//
		// Output Key - Word
		// Output Value - 1 i.e. one occurrence of a word
		// ----------------------------------------------------------------------------------------
		String line = value.toString();
		Pattern pattern = Pattern.compile("[A-Za-z]+");
		Matcher matcher = pattern.matcher(line);
		while (matcher.find()) {
			context.write(new Text(matcher.group()), new IntWritable(1));
		}
		
	}

}
