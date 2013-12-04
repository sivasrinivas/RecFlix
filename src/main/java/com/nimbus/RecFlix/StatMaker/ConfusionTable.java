/**
 * 
 */
package com.nimbus.RecFlix.StatMaker;

/**
 * @author prabal
 *
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/* Confusion Table
 *      -ve +ve
 * 	-ve	 a   b
 * 	+ve	 c   d
 */
public class ConfusionTable {

	public static class Map extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		// private IntWritable mvID = new IntWritable();
		private DoubleWritable sqr = new DoubleWritable();

		/* format of line: UserID, TruePositives(d) | TrueNegatives(a) | FalseNegatives(b) | FalsePositives(c) */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split(",");
			
			context.write(NullWritable.get(), new Text(words[1]));
		}
	}

	public static class Reduce extends
			Reducer<NullWritable, Text, NullWritable, Text> {

		public void reduce(NullWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			long sumA = 0;
			long sumB = 0;
			long sumC = 0;
			long sumD = 0;
					
			for (Text val : values) {
				String[] confusionVals = val.toString().split("|");
				
				// d | a | b | c
				if (confusionVals.length == 4) {
					sumD += Long.parseLong(confusionVals[0]);
					sumA += Long.parseLong(confusionVals[1]);
					sumB += Long.parseLong(confusionVals[2]);
					sumC += Long.parseLong(confusionVals[3]);
				}
			}
			
			// accuracy = correct recommendations / total recommendations
			double accuracy = (double) (sumD + sumA) / (double) (sumA + sumB + sumC + sumD);
			
			// Mean absolute error
			double mae = (double) (sumB + sumC) / (double) (sumA + sumB + sumC + sumD);
			
			String eval = String.format("Accuracy: %.2f, Mean Absolute Error: %.2f", accuracy, mae);
			
			context.write(NullWritable.get(), new Text(eval));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "ConfusionTable");

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setJarByClass(ConfusionTable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Confusion directory
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// output directory
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
