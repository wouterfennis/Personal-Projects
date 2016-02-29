package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
		String[]words = value.toString().split("\\s");

			// loop through all the words
			for (String word : words) {
				// first we convert the word to lower case characters
				word.toLowerCase();
				word.replaceAll("[^A-Za-z0-9 ]","");
				// loop through every char of the word
				int i = 0;
				int wordLength = word.length();
				while(i < wordLength){

					// retrieve the current character (but still use the string format)
					String character = word.charAt(i) + "";
					String nextCharacter = "";
					// make sure there is a next character
					if(i + 1 < wordLength ){
						// we can now safely do '+ 1' without risking 'StringIndexOutOfBounds'
						nextCharacter = word.charAt(i + 1) + "";
					}

					// only write if there is a nextCharacter
					//if(nextCharacter != ""){
						context.write(new Text(character), new Text(nextCharacter));
					//}
					// raise the index for the next character
					i++;
				}
		}
	}
}

class LetterFrequencyReducer extends Reducer<Text, Text, Text, Text> {
	private int[] occurences;
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		occurences = new int[28];
		final String[] ALPHABET = {"a","b","c","d","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","x"};


		String nextCharactersNumber = "";

		for (Text letter : values) {
			int index = Arrays.asList(ALPHABET).indexOf(letter.toString());
			// if the character is not found the index will be '-1'
			if(index != -1){
				// increment the value at the right index
				occurences[index] = occurences[index] + 1;
			}

		}
		int totalOccurences = 0;
		for(int occurence : occurences){
			nextCharactersNumber = nextCharactersNumber + " " + occurence;
			totalOccurences = totalOccurences + occurence;
		}
		nextCharactersNumber = nextCharactersNumber + " Total --> " + totalOccurences;


		context.write(key, new Text(nextCharactersNumber));
	}
}
