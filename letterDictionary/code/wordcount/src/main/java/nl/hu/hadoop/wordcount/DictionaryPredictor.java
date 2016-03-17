package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.text.Normalizer;
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
        //convert line to readable characters
        String line = value.toString().trim().replaceAll(" +", " ");
        line = Normalizer.normalize(line, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
        line = line.toLowerCase().replaceAll("[^A-Za-z ]", "");
        // retrieve the words from the line
        String[] words = line.split("\\s");

        for (String word : words) {
            // first we convert the word to lower case characters and replace any non-alphabet characters
            context.write(new Text(line), new Text(word));
        }
    }
}

class DictionaryPredictorReducer extends Reducer<Text, Text, Text, Text> {
    final static String LETTERFREQUENCY_FILE_PATH = "/media/wouter/Extra/hadoop-2.7.2/letterfrequentie-ENGELS.txt";
    private DictionaryWordPredictor dictionaryWordPredictor = new DictionaryWordPredictor(LETTERFREQUENCY_FILE_PATH);

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numberOfWords = 0;
        double sumOfWordPercentage = 0;

        for(Text word : values) {
            double wordPercentage = dictionaryWordPredictor.predict(word.toString());
            sumOfWordPercentage = sumOfWordPercentage + wordPercentage;
            numberOfWords++;
        }
        // calculate average percentage for whole sentence
        double averageLinePercentage = sumOfWordPercentage / numberOfWords;

        //round of to two decimals behind comma
        averageLinePercentage = (double) Math.round(averageLinePercentage * 100) / 100;

        context.write(key, new Text(averageLinePercentage + "%"));
    }

/*	@Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text(" "), new Text(" "));
	}*/
}
