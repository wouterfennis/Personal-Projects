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
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);


		job.waitForCompletion(true);
	}
}

class BevriendeGetallenMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		// retrieve the words from the line
		String[]numbers = value.toString().split("\\s");

		int i = 0;
		int quantityOfNumbers = numbers.length;
		// loop through all the numbers
		while (i < quantityOfNumbers) {

			int nextNumber = 0;

			// make sure there is a next number
			if(i + 1 < quantityOfNumbers){
				// we can now safely do "i+1" without "ArrayOutOfBounds"
				nextNumber = Integer.parseInt(numbers[i+1]);
			}

			context.write(new IntWritable(Integer.parseInt(numbers[i])), new IntWritable(nextNumber));
			i++;
		}
	}
}

class BevriendeGetallenReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

	private DivisorCalculator divisorCalculator = new DivisorCalculator();

	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int keyNumber = Integer.parseInt(key.toString());

		for (IntWritable value : values) {
			int possibleFriendNumber = Integer.parseInt(value.toString());
			ArrayList<Integer> divisors = divisorCalculator.calculateDivisors(possibleFriendNumber);

			int sumOfDivisors = calculateSumOfDivisors(divisors);
			String friendNumberAnswer = "\tis geen bevriend nummer van -->\t";
			if(sumOfDivisors == keyNumber){
				friendNumberAnswer = "\tis een bevriend nummer van -->\t";
			}
			context.write(new Text(keyNumber + friendNumberAnswer), new IntWritable(possibleFriendNumber));
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
