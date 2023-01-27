package com.chandra.otari.alphabet;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.chandra.otari.alphabet.map.AlphabetMapper;
import com.chandra.otari.alphabet.reduce.AlphabetReducer;

public class Alphabet {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.out.println("Usage: Alphabet <Input Path> <Output Path>");
			System.exit(-1);
		}
		
		Configuration configuration = new Configuration();
		
		Job job = Job.getInstance(configuration, "Alphabet");
		job.setJarByClass(Alphabet.class);
	
		job.setMapperClass(AlphabetMapper.class);
		job.setReducerClass(AlphabetReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
