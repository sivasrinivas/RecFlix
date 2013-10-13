package com.nimbus.RecFlix.ItemBased;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class ReduceValue1{
	public int item_count;
	public int item_sum;
	public HashMap<String,Float> list;
	ReduceValue1(int item_count,int item_sum, HashMap<String,Float> list){
		this.item_count = item_count;
		this.item_sum = item_sum;
		this.list = list;
	}
	ReduceValue1(int item_count,int item_sum){
		this.item_count = item_count;
		this.item_sum = item_sum;
		this.list = null;
	}
}

class ReduceValue2{
	float corr_sim;
    float cos_sim;
    float reg_corr_sim;
    float jaccard_sim;
    int n;
    ReduceValue2(float corr_sim, float cos_sim, float reg_corr_sim, float jaccard_sim, int n){
    	this.corr_sim = corr_sim;
    	this.cos_sim = cos_sim;
    	this.reg_corr_sim = reg_corr_sim;
    	this.jaccard_sim = jaccard_sim;
    	this.n = n;
    }
}
public class CalculateSimilarity {

	public static ArrayList<HashMap<String,Float>> combination(HashMap<String,Float> val){
		return null;
	}
	
	public static float correlation(int n, float sum_xy,float sum_x,float sum_y,float sum_xx,float sum_yy){
		return 0;
	}
	
	public static float regularized_correlation(int n,float sum_xy,float sum_x,float sum_y,float sum_xx,float sum_yy, int k, int j){
		return 0;
	}
	
	public static float cosine(float sum_xy, double x, double y){
		return 0;
	}
	
	public static class Map extends Mapper<LongWritable, Text, Set<String>, Collection<Float>> {

	//	private Text word = new Text();

		public void map(String user_id, ReduceValue1 value, Context context)
				throws IOException, InterruptedException {
			int item_count = value.item_count,item_sum = value.item_sum;
			HashMap<String,Float> list = value.list;
			
				        for (HashMap<String,Float> combi : combination(list)){
				        	
				                    context.write(combi.keySet(), combi.values());
				        }
		}
	}

	public static class Reduce extends
	Reducer<Text, IntWritable, Set<String>, ReduceValue2> {

		public void reduce(Set<String> user_pair, Iterable<Collection<Float>> ratings, Context context)
				throws IOException, InterruptedException {

			       float sum_xx = 0, sum_xy = 0, sum_yy = 0, sum_x = 0, sum_y = 0;
			       int n = 0;
			       Iterator<String> it1 = user_pair.iterator();
			       String item_xname = it1.next();
			       String item_yname = it1.next();  
			       for (Collection<Float> rating: ratings){
			    	   Iterator<Float> it2 = rating.iterator();
			    	   Float item_x = it2.next();
				       Float item_y = it2.next();
				       sum_xx += item_x * item_x;
				               sum_yy += item_y * item_y;
				               sum_xy += item_x * item_y;
				               sum_y += item_y;
				               sum_x += item_x;
				               n += 1;
			       }
			
			      float corr_sim = correlation(n, sum_xy, sum_x, sum_y, sum_xx, sum_yy);

			       float reg_corr_sim = regularized_correlation(n, sum_xy, sum_x, sum_y, sum_xx, sum_yy, 10, 0);

			       float cos_sim = cosine(sum_xy, Math.sqrt(sum_xx), Math.sqrt(sum_yy));

			        float jaccard_sim = 0;

			context.write(user_pair, new ReduceValue2(corr_sim,cos_sim, reg_corr_sim, jaccard_sim, n));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "calculate_similarity");
		job.setJarByClass(CalculateSimilarity.class);
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
