package com.nimbus.RecFlix.ItemBased;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.nimbus.RecFlix.ItemBased.CalculateSimilarity.Map;
import com.nimbus.RecFlix.ItemBased.CalculateSimilarity.Reduce;

public class CalculateRanking {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        		//itemId1, itemId2, similarity, count
        		String line = value.toString();
        		
        		StringTokenizer st = new StringTokenizer(line);
        		try{
        			String[] parts1 = st.nextToken().split(",");
            		String[] parts2 = st.nextToken().split(",");
            		
        			if(Double.parseDouble(parts2[0])>0){
            			context.write(new Text(parts1[0]+","+parts2[0]), new Text(parts1[1]+","+parts2[1]));
            		}
        		}catch(Exception e){
        			System.out.println("Got exception :)");
        		}
        		
        }
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text keySim, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            	
            	for(Text value : values){
            		String similarCounts = value.toString();
            		//item1, similarity
            		String[] parts1 = keySim.toString().split(",");
            		//item2, count
            		String[] parts2 = similarCounts.split(",");
            		
            		context.write(new Text(parts1[0]+","+parts2[0]), new Text(parts1[1]+","+parts2[1]));
            		
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
		Job job = new Job(conf, "calc_ranking");
		job.setJarByClass(CalculateRanking.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}

}