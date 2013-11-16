package com.nimbus.RecFlix.UserBased;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserSimilarity {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text key = new Text();
		private Text value = new Text();

		public void map(LongWritable key1, Text value, Context context)	throws IOException, InterruptedException {

			
			//itemId, userCount, userRatingSum,(userId, rating,...)
			String line = value.toString();
			
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String[] row = tokenizer.nextToken().split("|");
				String[] row1 = row[1].split(",(");
				String[] rating = row1[1].substring(0, row[1].length() - 2)
						.split(" ");
				// String item_count = row[1], item_sum = row[2], rating =
				// row[3];
				if (rating.length > 1) {
					combination(rating);
					for (List<String> combi : lists) {
						String[] s1 = combi.get(0).split(",");
						String[] s2 = combi.get(1).split(",");
						key.set(s1[0] + "," + s2[0]);
						value.set(s1[1] + "," + s2[1]);
						context.write(key, value);
					}
					lists = new ArrayList<List<String>>();
				}
			}
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "User Similarity");
		job.setJarByClass(UserSimilarity.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
