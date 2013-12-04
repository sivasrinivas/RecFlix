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
public class MergeHelper {
	
	public static class Map extends Mapper<NullWritable, BytesWritable, NullWritable, Text> {
		
//		Text movieKey = new Text();
		Text movieVal = new Text();
		
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String content = new String(value.getBytes());
			StringTokenizer tokenizer = new StringTokenizer(content);
			String movieID = null;
			if (tokenizer.hasMoreTokens()) {
				movieID = (tokenizer.nextToken()).split(":")[0];
				//movieKey.set(movieID);
			}
			
			int count = 0;
			if (movieID != null) {
				while (tokenizer.hasMoreTokens()) {
					String movieInfo = tokenizer.nextToken();
					// reached last (empty) line
					if (count == 80 || movieInfo == null || movieInfo.trim().length() == 0)
						break;
					
					String[] words = movieInfo.split(",");
					movieInfo = String.format("%s,%s,%s", words[0], movieID, words[1]);
					movieVal.set(movieInfo);
					context.write(NullWritable.get(), movieVal);
					count++;
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text> {

		public void reduce(NullWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values)
				context.write(NullWritable.get(), value);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MergetoCSV");
		job.setJarByClass(MergeHelper.class);
		
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
