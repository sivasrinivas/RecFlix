package com.nimbus.RecFlix.DatasetMerge;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DatasetMerge {
	public static class Map extends Mapper<LongWritable, Text, Set<String>, Collection<Double>> {

		private Text word = new Text();
			
		public void map(LongWritable userId, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			while (tokenizer.hasMoreTokens()) {
				String s = tokenizer.nextToken();
				// Extract title from imdb/netflix dataset from distributed cache
				
				// call similarity functions and fetch; Could improve to singleton instance
				JaccardSimilarity jSim = new JaccardSimilarity();
				LevenshteinDistance lDist = new LevenshteinDistance();
				//jSim.compare(s, t);
				//lDist.compare(s, t);
				
				word.set(s);
			}
			
		}
	}

		public static class Reduce extends
		Reducer<Text, IntWritable, Set<String>, Collection<Double>> {

			public void reduce(Set<String> user_pair, Iterable<Collection<Float>> ratings, Context context)
					throws IOException, InterruptedException {
				      
			}
		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = new Job(conf, "Dataset Merge");
			job.setJarByClass(DatasetMerge.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		}
}