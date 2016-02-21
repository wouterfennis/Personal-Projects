package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class BevriendeGetallen {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(BevriendeGetallen.class);

		// possible fix for empty output file
		job.getConfiguration().set("mapreduce.ifile.readahead", "false");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(BevriendeGetallenMapper.class);
		job.setReducerClass(BevriendeGetallenReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}

class BevriendeGetallenMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		// retrieve the words from the line
		String[]numbers = value.toString().split("\\s");

		int i = 0;
		// loop through all the numbers
		while (i < numbers.length) {
			context.write(new IntWritable(Integer.parseInt(numbers[i])), new IntWritable(Integer.parseInt(numbers[i+1])));
			i++;
		}
	}
}

class BevriendeGetallenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private DivisorCalculator divisorCalculator = new DivisorCalculator();

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int keyNumber = Integer.parseInt(key.toString());

		for (IntWritable value : values) {
			int possibleFriendNumber = Integer.parseInt(value.toString());
			ArrayList<Integer> divisors = divisorCalculator.calculateDivisors(possibleFriendNumber);

			int sumOfDivisors = calculateSumOfDivisors(divisors);

			if(sumOfDivisors == keyNumber){
				context.write(key, new IntWritable(possibleFriendNumber));
			}
		}

	}

	private int calculateSumOfDivisors(ArrayList<Integer> divisors){

		int sumOfDivisors = 0;
		for(int divisor : divisors){
			sumOfDivisors = sumOfDivisors + divisor;
		}
		return sumOfDivisors;
	}
}
