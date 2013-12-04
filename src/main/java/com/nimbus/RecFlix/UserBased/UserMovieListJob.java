package com.nimbus.RecFlix.UserBased;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class UserMovieListJob {

	// Mapper class
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			Text user = new Text();
			Text movie = new Text();

			String line = value.toString();
			// tokenize the input string and add each word
			StringTokenizer tokenizer = new StringTokenizer(line,",");
			
			user.set(tokenizer.nextToken());
			movie.set(tokenizer.nextToken()+"xxx"+tokenizer.nextToken());
			output.collect(user, movie);
			
		}
	}

	// Reducer class
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			StringBuilder movies = new StringBuilder();
			// count sum and set it in output
			while (values.hasNext()) {
				String movieRating = values.next().toString();
				movies.append(movieRating+",");
			}
			String finalString = movies.toString();
			if(finalString.length()>1){
				finalString=finalString.substring(0, movies.length()-1);
			}
			output.collect(key, new Text(finalString));
		}
		
	}

	public static void main(String[] args) throws Exception {
		// Create a job with configurations
		JobConf conf = new JobConf(UserMovieListJob.class);
		conf.setJobName("User Movie List");

		// setting output datatypes
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// Mapper and reducer classes
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		// Input Output format classes
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// set the input and output file paths
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}