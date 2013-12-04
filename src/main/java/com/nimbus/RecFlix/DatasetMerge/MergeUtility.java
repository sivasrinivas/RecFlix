/**
 * 
 */
package com.nimbus.RecFlix.DatasetMerge;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Combines all input files into one output with desired format
 * @author prabal
 *
 */
public class MergeUtility {
	
	// private IntWritable mvID = new IntWritable();

	// movie-id - movie-title pair
	public static HashMap<Integer, String> movieCache = new HashMap<Integer, String>();
	// movie-id - year pair
	public static HashMap<Integer, Integer> yearCache = new HashMap<Integer, Integer>();

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
				// movieid, release year, movie title (title may contain comma's)
				String title = "";
				if (words.length == 3)
					title = words[2];
				else if (words.length > 3) {
					for (int i = 2; i < words.length - 1; i++)
						title += words[i] + ",";
					
					title += words[words.length - 1];
				}
					
				if (title == "")
					return;
				
				Integer mId = Integer.parseInt(words[0]);
				movieCache.put(mId, title);
				yearCache.put(mId, Integer.parseInt(words[1]));
			}
		} finally {
			fis.close();
		}
	}
	
	
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {
		
		JaccardSimilarity jSim = new JaccardSimilarity();
		LevenshteinDistance lDist = new LevenshteinDistance();
		
		StringBuilder s = new StringBuilder();
		Text movieKey = new Text();
		Text movieVal = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String content = new String(value.getBytes());
			
			String line = value.toString();
			String[] mvInfo = line.split("\\d{4}");
			
			String movieTitle = mvInfo[0].trim();
			String genre = mvInfo[2].trim();
			
			if (movieTitle != null && movieTitle != "") {
				double min = Double.POSITIVE_INFINITY;
				String matchedMovie = "";
				int movieID = 0;
				
				for (String movie : movieCache.values()) {
					double val = jSim.compare(movie, movieTitle);
					if (min > val) {
						min = val;
						matchedMovie = movie;
					}
				}
				
				for (int mid : movieCache.keySet()) {
					if (movieCache.containsValue(matchedMovie)) {
						movieID = mid;
						break;
					}
				}
				
				String output = String.format("%d,%s,%s,%s", movieID, yearCache.get(movieID), matchedMovie, genre);
				context.write(NullWritable.get(), new Text(output));
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Sample test file
		DistributedCache.addCacheFile(new URI(args[1]), conf);
		Job job = new Job(conf, "GenreAddition");
		job.setJarByClass(MergeUtility.class);
		
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		//job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
