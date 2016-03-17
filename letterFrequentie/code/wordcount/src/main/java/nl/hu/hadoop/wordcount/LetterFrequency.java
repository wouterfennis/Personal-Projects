package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class LetterFrequency {
	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(LetterFrequency.class);

		// possible fix for empty output file
		job.getConfiguration().set("mapreduce.ifile.readahead", "false");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(LetterFrequencyMapper.class);
		job.setReducerClass(LetterFrequencyReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
	}
}

class LetterFrequencyMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		// retrieve the words from the line
		String[]persons = value.toString().split("\\,");

		String mainPerson = persons[0];
		ArrayList<String> friendsOfMainPerson = new ArrayList<String>();

		for(int i = 1; i < persons.length; i++){
			context.write(new Text(mainPerson), new Text(persons[i]));
		}
	}
}

class LetterFrequencyReducer extends Reducer<Text, Text, Text, ArrayWritable> {
	private HashMap personsWithFriends = new HashMap();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String mainPerson = key.toString();
		ArrayList<String> friendsOfMainPerson = new ArrayList<String>();
		for(Text friendOfMainPerson : values){
			friendsOfMainPerson.add(friendOfMainPerson.toString());
		}

		if(!personsWithFriends.containsKey(mainPerson)){
			personsWithFriends.put(mainPerson, friendsOfMainPerson);
		}

		for(String friend : friendsOfMainPerson){
			ArrayList<String> commonFriends = new ArrayList<String>();
			if(personsWithFriends.containsKey(friend)){
				//friend is aanwezig
				ArrayList<String> friendsOfFriend = (ArrayList<String>)personsWithFriends.get(friend);
				for(String friendOfFriend : friendsOfFriend){
					if(friendsOfMainPerson.contains(friendOfFriend)){
						commonFriends.add(friendOfFriend);
					}
				}
				context.write(new Text(mainPerson + "|" + friend), new ArrayWritable((String[])commonFriends.toArray()));
			} else {
				// friend is niet aanwezig
			}
		}
}
}
