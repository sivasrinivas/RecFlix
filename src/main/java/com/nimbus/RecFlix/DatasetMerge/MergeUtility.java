/**
 * 
 */
package com.nimbus.RecFlix.DatasetMerge;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Combines all input files into one output with desired format
 * @author prabal
 *
 */
public class MergeUtility {
	
	private Text word = new Text();
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		public void map(LongWritable userId, Text value, Context context)
				throws IOException, InterruptedException {
		
		}
	
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Set<String>, Collection<Double>> {

		public void reduce(Set<String> user_pair, Iterable<Collection<Float>> ratings, Context context)
				throws IOException, InterruptedException {
			      
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MergetoCSV");
		job.setJarByClass(DatasetMerge.class);
		
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
