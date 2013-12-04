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
	
	//private Text word = new Text();
	
	
	public static class Map extends Mapper<NullWritable, BytesWritable, NullWritable, Text> {
		
//		private Text filename;
//		
//		@Override
//		protected void  setup(Context context) throws IOException, InterruptedException {
//			InputSplit split = context.getInputSplit();
//			Path path = ((FileSplit) split).getPath();
//			filename = new Text(path.toString());
//		}
		
		StringBuilder s = new StringBuilder();
		Text movieKey = new Text();
		Text movieVal = new Text();
		
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String content = new String(value.getBytes());
//			String[] lines = content.split("\n");
//			
//			String movieID = (lines[0].split(":"))[0];
//			
//			System.out.println(String.format("Hello:%s,Length:%d", movieID, lines.length));
//			
//			for (int i = 1; i < lines.length - 2; i++) {
//				movieKey.set(movieID);
//				Text movieInfo = lines[i];
//				context.write(movieKey, lines[i])
//			}
			
			StringTokenizer tokenizer = new StringTokenizer(content);
			String movieID = null;
			if (tokenizer.hasMoreTokens()) {
				movieID = (tokenizer.nextToken()).split(":")[0];
				movieKey.set(movieID);
			}
			
			if (movieID != null) {
				while (tokenizer.hasMoreTokens()) {
					String movieInfo = tokenizer.nextToken();
					// reached last (empty) line
					if (movieInfo == null || movieInfo.trim().length() == 0)
						break;
					
					String[] words = movieInfo.split(",");
					movieInfo = String.format("%s,%s,%s", words[0], movieID, words[1]);
					movieVal.set(movieInfo);
					context.write(NullWritable.get(), movieVal);
				}
			}
		}
		
		
//		public void map(NullWritable key, BytesWritable value, Context context)
//				throws IOException, InterruptedException {
//			String content = new String(value.getBytes());
//			String[] lines = content.split("\n");
//			
//			String movieID = (lines[0].split(":"))[0];
//			
//			System.out.println(String.format("Hello:%s,Length:%d", movieID, lines.length));
//			
//			//StringBuilder s = new StringBuilder();
//			
//			for (int i = 1; i < lines.length - 2; i++) {
//				String[] words = lines[i].split(",");
//				// ensure words.length = 3 (customerid, rating, date)
//				s.append(String.format("%s,%s,%s\n", words[0], movieID, words[1]));
//			}
//			// process last two lines
//			String[] lastButOne = lines[lines.length - 2].split(",");
//			s.append(String.format("%s,%s,%s", lastButOne[0], movieID, lastButOne[1]));
//			
//			// incase last line is empty line in input files then do not write to output
//			String[] last = lines[lines.length - 1].split(",");
//			if (last != null && last.length == 3)
//				s.append(String.format("\n%s,%s,%s", last[0], movieID, last[1]));
//			
//			//BytesWritable data = new BytesWritable(s.toString().getBytes());
//			//context.write(key, data);
//			context.write(key, new Text(s.toString()));
			
			
//			String content = new String(value.getBytes());
//			String[] lines = content.split("\n");
//			
//			String movieID = (lines[0].split(":"))[0];
//			
//			System.out.println(String.format("Hello:%s,Length:%d", movieID, lines.length));
//			
//			List<String> buffer = new ArrayList<String>();
//			
//			for (int i = 1; i < lines.length - 2; i++) {
//				String[] words = lines[i].split(",");
//				// ensure words.length = 3 (customerid, rating, date)
//				buffer.add(String.format("%s,%s,%s\n", words[0], movieID, words[1]));
//			}
//			// process last two lines
//			String[] lastButOne = lines[lines.length - 2].split(",");
//			buffer.add(String.format("%s,%s,%s", lastButOne[0], movieID, lastButOne[1]));
//			
//			// incase last line is empty line in input files then do not write to output
//			String[] last = lines[lines.length - 1].split(",");
//			if (last != null && last.length == 3)
//				buffer.add(String.format("\n%s,%s,%s", last[0], movieID, last[1]));
//			
//			String s = concatenateItems(buffer);
//			
//			//BytesWritable data = new BytesWritable(s.toString().getBytes());
//			//context.write(key, data);
//			context.write(key, new Text(s));
//			
//		}
		
//		public static String concatenateItems(List<String> items) {
//			if (items == null) 
//				return null;
//			if (items.size() == 0) 
//				return "";
//			
//			int size = 0;
//			for (String item : items)
//				size += item.length();
//			
//			StringBuilder result = new StringBuilder(size);
//			for (String item : items)
//				result.append(item);
//			
//			return result.toString();
//		}
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
		job.setJarByClass(MergeUtility.class);
		
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
