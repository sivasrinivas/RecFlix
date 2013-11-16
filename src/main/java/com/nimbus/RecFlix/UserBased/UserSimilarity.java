package com.nimbus.RecFlix.UserBased;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.nimbus.RecFlix.UserBased.UserRatings.Map;
import com.nimbus.RecFlix.UserBased.UserRatings.Reduce;

class MapperValueRatings{
	int userCount;
	int userRatingSum;
	HashMap<Integer, Float> ratings;
	
	public MapperValueRatings(String input){
		System.out.println(input);
		userCount=0;
		userRatingSum=0;
		ratings = new HashMap<Integer, Float>();
	}
	
}

class ReducerValueSimilarity{
	float similarity;
	public ReducerValueSimilarity(){
		similarity = 0.0f;
	}
}

class UserRating{
	long userId;
	double rating;
}

class UserPair{
	long userId1;
	long userId2;
	double rating1;
	double rating2;
	public UserPair(long userId1, long userId2, double rating1, double rating2){
		this.userId1 = userId1;
		this.userId2 = userId2;
	}
	
	public UserPair(){
		userId1=0;
		userId2=0;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (userId1 ^ (userId1 >>> 32));
		result = prime * result + (int) (userId2 ^ (userId2 >>> 32));
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof UserPair)) {
			return false;
		}
		UserPair other = (UserPair) obj;
		if (userId1 != other.userId1) {
			return false;
		}
		if (userId2 != other.userId2) {
			return false;
		}
		return true;
	}
	
	
}

public class UserSimilarity {
	
	//combinations
	public static List<List<String>> lists = new ArrayList<List<String>>();
	public static void permutate(String[] arr, int k) {
		internalPermutate(arr, k, 0, 0);
	}

	public static void internalPermutate(String[] arr, int k, int step, int index) {
		if (step == k) {
			List<String> list = new ArrayList<String>();
			for (int i = 0; i < k; i++) {
				list.add(arr[i]);
			}
			lists.add(list);
		}

		for (int i = step + index; i < arr.length; i++) {
			swap(arr, step, i);
			internalPermutate(arr, k, step + 1, i);
			swap(arr, step, i);
		}
	}

	public static void swap(String[] arr, int x, int y) {
		String temp = arr[x];
		arr[x] = arr[y];
		arr[y] = temp;
	}

	public static ArrayList<HashMap<String,Float>> combination(String[] val){
		
		internalPermutate(val, 2, 0, 0);
		return null;
	}
	//end of combinations functions
	

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable itemId, Text value, Context context)	throws IOException, InterruptedException {
			Text userPair = new Text();
			Text ratingPair = new Text();
			
			try{
				//itemId, userCount, userRatingSum,(userId, rating,...)
				System.out.println("values---->"+value);
				String line = value.toString();
				String userCount = line.substring(0, line.indexOf(","));
				String userRatingSum = line.substring(line.indexOf(",")+1,line.indexOf(",(") );
				String[] ratings = line.substring(line.indexOf(",(")+2, line.indexOf(")")).split(" ");
				//get combinations for array of (userId,rating)
				if(ratings.length>1){
					combination(ratings); /*combinations will be set in lists variable*/
					
					for (List<String> combi : lists){
						String[] s1 = combi.get(0).split(",");
						String[] s2 = combi.get(1).split(",");
						userPair.set(s1[0]+","+s2[0]);
						ratingPair.set(s1[1]+","+s2[1]);
						//writing to context userPair and their ratings pair
						context.write(userPair, ratingPair);
					}
					lists = new ArrayList<List<String>>();
				}
			}catch(Exception e){
				System.out.println("Got exception :)");
			}
			
		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text userPair, Iterable<Text> ratings, Context context)	throws IOException, InterruptedException {
			Text similarity = new Text();
			double sum_xx = 0.0, sum_xy = 0.0, sum_yy = 0.0, sum_x = 0.0, sum_y = 0.0;
			int count=0;
			
			for (Text rating: ratings){
				String[] r = rating.toString().split(",");
				double item_x = Double.parseDouble(r[0]);
				double item_y = Double.parseDouble(r[1]);
				
				sum_xx += item_x * item_x;
				sum_yy += item_y * item_y;
				sum_xy += item_x * item_y;
				sum_y += item_y;
				sum_x += item_x;
				count ++;
			}
			
			double numerator = count*sum_xy - sum_x*sum_y;
			double denominator = Math.sqrt(count*sum_xx - sum_x*sum_x)*Math.sqrt(count*sum_yy - sum_y*sum_y);
			double corr_sim = numerator/denominator;
			
			similarity.set(corr_sim+","+count);
			context.write(userPair, similarity);
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
		Job job = new Job(conf, "User Similarity");
		job.setJarByClass(UserSimilarity.class);
		
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
