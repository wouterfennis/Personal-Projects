package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.*;

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

	private DivisorCalculator divisorCalculator = new DivisorCalculator();

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {

		int number = Integer.parseInt(value.toString());
		ArrayList<Integer> divisors = divisorCalculator.calculateDivisors(number);
		int sumOfDivisors = calculateSumOfDivisors(divisors);

		context.write(new IntWritable(number), new IntWritable(sumOfDivisors));
	}

	private int calculateSumOfDivisors(ArrayList<Integer> divisors){

		int sumOfDivisors = 0;
		for(int divisor : divisors){
			sumOfDivisors = sumOfDivisors + divisor;
		}
		return sumOfDivisors;
	}
}

class BevriendeGetallenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	private HashMap<Integer, Integer> numbers = new HashMap();


	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

		int keyNumber = Integer.parseInt(key.toString());
		int sumOfDivisors = values.iterator().next().get();

			if (numbers.containsKey(sumOfDivisors)) {
				if (keyNumber == numbers.get(sumOfDivisors)) {
					context.write(new IntWritable(sumOfDivisors), new IntWritable(keyNumber));
				}
			} else {
				numbers.put(keyNumber, sumOfDivisors);
			}
	}
}
