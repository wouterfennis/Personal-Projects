package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class DictionaryPredictor {

    public static void main(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(DictionaryPredictor.class);

        // possible fix for empty output file
        job.getConfiguration().set("mapreduce.ifile.readahead", "false");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(DictionaryPredictorMapper.class);
        job.setReducerClass(DictionaryPredictorReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}

class DictionaryPredictorMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
        // retrieve the words from the line
        String[] words = value.toString().split("\\s");

        // loop through all the words
        for (String word : words) {
            // first we convert the word to lower case characters
            word = word.toLowerCase().replaceAll("[^A-Za-z0-9 ]", "");
            context.write(new Text(word), new Text(word));
        }
    }
}

class DictionaryPredictorReducer extends Reducer<Text, Text, Text, Text> {
    final static String LETTERFREQUENCY_FILE_PATH = "";
    private DictionaryWordPredictor dictionaryWordPredictor = new DictionaryWordPredictor(LETTERFREQUENCY_FILE_PATH);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        double wordChance = dictionaryWordPredictor.predict(key.toString());

        double wordChanceProcent = wordChance * 100;
        context.write(key, new Text(wordChanceProcent + ""));
    }

/*	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		String bottomTotals = "";
		for(int totalBottomOccurence : totalBottomOccurences){
			bottomTotals = bottomTotals + calculateWhitespace(totalBottomOccurence) + totalBottomOccurence;
		}
		context.write(new Text("------"), new Text(bottomTotals.replaceAll(".","-")));
		context.write(new Text(" "), new Text(bottomTotals));
	}*/
}
