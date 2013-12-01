package com.nimbus.RecFlix.UserBased;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class reads input from dataset and emits list of users and their ratings
 * @author siva
 */
public class UserRatings {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
//			System.out.println(line);
			Text itemIdText = new Text();
			Text userIdRatingText = new Text();
			
			String[] parts = line.split(",");
			String itemId =parts[1];
			String userId = parts[0];
			String rating = parts[2];
			
			userIdRatingText.set(userId+","+rating);
			itemIdText.set(itemId);
			context.write(itemIdText, userIdRatingText);
		}
	}
	
	public static class Reduce extends	Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text itemId, Iterable<Text> values, Context context)	throws IOException, InterruptedException {
			try{
				StringBuilder builder = new StringBuilder();
				int userCount = 0;
				int userRatingSum = 0;
				Text userRating = new Text();
				for(Text value : values){
					//0-userId, 1-rating
					String[] parts = value.toString().split(",");
					userCount ++;
					userRatingSum+=Integer.parseInt(parts[1]);
					builder.append(parts[0]+","+parts[1]+" ");
				}
				//remove last extra space
				builder.deleteCharAt(builder.length()-1);
				
				userRating.set(userCount+","+userRatingSum+",("+builder.toString()+")");
				context.write(itemId, userRating);
			}catch(Exception e){
				System.out.println("Got exception :)");
			}
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "User Ratings");
		job.setJarByClass(UserRatings.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.out.println(args[0]);
		System.out.println(args[1]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
