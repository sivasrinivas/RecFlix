package com.nimbus.RecFlix.ItemBased;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class ReduceValue{
	public int item_count;
	public int item_sum;
	public HashMap<String,Float> list;
	ReduceValue(int item_count,int item_sum, HashMap<String,Float> list){
		this.item_count = item_count;
		this.item_sum = item_sum;
		this.list = list;
	}
}

class MapValue{

	public float rating;
	public String item_id;
	MapValue(String rating,String item_id){
		this.rating = Float.parseFloat(rating);
		this.item_id = item_id;
	}
}
public class CountRatings {

	
	public static class Map extends Mapper<LongWritable, Text, String, MapValue> {

		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				String[] row = tokenizer.nextToken().split("|"); 
				String user_id = row[0], item_id = row[1], rating = row[2];
				MapValue mapVal = new MapValue(rating,item_id);
				word.set(tokenizer.nextToken());
				context.write(user_id, mapVal);
			}
		}
	}

	public static class Reduce extends
	Reducer<Text, IntWritable, String, ReduceValue> {

		public void reduce(String user_id, Iterable<MapValue> values, Context context)
				throws IOException, InterruptedException {

			int item_count = 0;
			int item_sum = 0;
			HashMap<String,Float> list = new HashMap<String,Float>();
			for (MapValue mapVal: values){
				item_count += 1;
				item_sum += mapVal.rating;
				list.put(mapVal.item_id, mapVal.rating);
			}
			ReduceValue redVal = new ReduceValue(item_count, item_sum, list);

			context.write(user_id, redVal);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "count_ratings");
		job.setJarByClass(CountRatings.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
