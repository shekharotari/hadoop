package com.chandra.otari.alphabet.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AlphabetReducer extends Reducer<IntWritable, Text, IntWritable, LongWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<Text> values,
			Reducer<IntWritable, Text, IntWritable, LongWritable>.Context context)
			throws IOException, InterruptedException {
//		System.out.println("Key=" + key);
		int wordCount = 0;
		for (Text value : values) {
//			System.out.println("value=" + value);
			wordCount++;
		}
		context.write(key, new LongWritable(wordCount));
	}
	
}
