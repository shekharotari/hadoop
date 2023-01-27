package com.chandra.otari.wordcount.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// ----------------------------------------------------------------------------
		// Input Key - Word
		// Input Values - List of number of times word appeared
		//
		// Iterate through the list of values and count the occurrences of the word.
		//
		// Output Key - Word
		// Output Value - Count of number of times word appeared
		// ----------------------------------------------------------------------------
		int finalCount = 0;
		for (IntWritable value : values) {
			finalCount += value.get();
		}
		context.write(key, new IntWritable(finalCount));
	}
	
}
