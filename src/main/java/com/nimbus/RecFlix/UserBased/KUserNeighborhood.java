package com.nimbus.RecFlix.UserBased;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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


public class KUserNeighborhood {

	/**
	 * @param args
	 * @author Siva
	 */
	
	// Mapper class
		public static class Map extends MapReduceBase implements
				Mapper<LongWritable, Text, Text, IntWritable> {
			private final static IntWritable one = new IntWritable(1);
			private UserProfile profile = new UserProfile();
			private Text text = new Text();

			public void map(LongWritable key, Text value,	OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
				String line = value.toString();
				// tokenize the input string and add each word
				StringTokenizer tokenizer = new StringTokenizer(line);
				while (tokenizer.hasMoreTokens()) {
					profile.setId(Integer.parseInt(tokenizer.nextToken()));
					output.collect(text, one);
				}
			}
		}

		// Reducer class
		public static class Reduce extends MapReduceBase implements
				Reducer<Text, IntWritable, Text, IntWritable> {
			public void reduce(Text key, Iterator<IntWritable> values,	OutputCollector<Text, IntWritable> output, Reporter reporter)
					throws IOException {
				int sum = 0;
				// count sum and set it in output
				while (values.hasNext()) {
					sum += values.next().get();
				}
				output.collect(key, new IntWritable(sum));
			}
		}

		public static void main(String[] args) throws Exception {
			// Create a job with configurations
			JobConf conf = new JobConf(KUserNeighborhood.class);
			conf.setJobName("kuserneighborhood");

			// setting output datatypes
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(IntWritable.class);

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
