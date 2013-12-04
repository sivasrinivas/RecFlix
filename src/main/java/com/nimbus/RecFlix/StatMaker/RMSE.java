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

public class RMSE {

	public static class Map extends
			Mapper<LongWritable, Text, NullWritable, DoubleWritable> {

		// private IntWritable mvID = new IntWritable();
		private DoubleWritable sqr = new DoubleWritable();

		// movieid - rating pair
		public static HashMap<Integer, Double> wordCache = new HashMap<Integer, Double>();

		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			Path[] paths = DistributedCache.getLocalCacheFiles(conf);

			for (Path file : paths) {
				readCache(file);
			}
		}

		private void readCache(Path path) throws IOException {
			BufferedReader fis = new BufferedReader(new FileReader(
					path.toString()));
			String line = null;

			try {
				while ((line = fis.readLine()) != null) {
					String[] words = line.split(",");
					wordCache.put(Integer.parseInt(words[1]),
							Double.parseDouble(words[2]));
				}
			} finally {
				fis.close();
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split(",");
			Integer movieID = Integer.parseInt(words[0]);

			if (wordCache.containsKey(movieID)) {
				double predictedRating = Double
						.parseDouble(words[words.length - 1]);
				double diff = predictedRating - wordCache.get(movieID);
				// mvID.set(movieID);
				sqr.set(diff * diff);
				context.write(NullWritable.get(), sqr);
			}
		}
	}

	public static class Reduce extends
			Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {

		public void reduce(NullWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {

			int count = 0;
			double sum = 0;
			for (DoubleWritable val : values) {
				count++;
				sum += val.get();
			}
			
			double avg = sum / count;
			double rmse = Math.sqrt(avg);
			
			context.write(NullWritable.get(), new DoubleWritable(rmse));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// Sample test file
		DistributedCache.addCacheFile(new URI(args[1]), conf);
		Job job = new Job(conf, "RMSE");

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setJarByClass(RMSE.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// Avg. ratings list file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// output directory
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
