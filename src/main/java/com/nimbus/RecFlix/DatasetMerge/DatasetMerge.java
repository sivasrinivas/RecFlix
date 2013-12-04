package com.nimbus.RecFlix.DatasetMerge;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

public class DatasetMerge {
	public static class Map extends Mapper<LongWritable, Text, NullWritable, Text> {

		private Text movieInfo = new Text();
		
		private int count = 0;
			
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
//			String line = value.toString();
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			
//			while (tokenizer.hasMoreTokens()) {
////				String s = tokenizer.nextToken();
//				// Extract title from imdb/netflix dataset from distributed cache
//				// do something
////				// call similarity functions and fetch; Could improve to singleton instance
////				JaccardSimilarity jSim = new JaccardSimilarity();
////				LevenshteinDistance lDist = new LevenshteinDistance();
////				//jSim.compare(s, t);
////				//lDist.compare(s, t);
////				word.set(s);
//						
//			}
			
			String line = value.toString();
			String[] words = line.split(",");
			
			// movieid, release year, movie title (title may contain comma's)
			String title = "";
			if (words.length == 3)
				title = words[2];
			else if (words.length > 3) {
				System.out.println("Line: " + key.toString() + " has comma(s) in the movie title... " + line);
				for (int i = 2; i < words.length - 1; i++)
					title += words[i] + ",";
				
				title += words[words.length - 1];
			}
				
			if (title == "")
				return;
			
			URL url = new URL("http://mymovieapi.com/?q=" + title.replaceAll(" ", "+"));
			try {
				//InputStream is = url.openStream();
				//JsonFactory jsonFactory = new JsonFactory();
				//JsonParser jp = jsonFactory.createJsonParser(url);
				
				ObjectMapper mapper = new ObjectMapper();
				JsonNode rootNode = mapper.readValue(url, JsonNode.class);

				if (rootNode != null && rootNode.isArray()) {
					//System.out.println("ROOT NODE >>> ARRAY ...");
					JsonNode genresNode = rootNode.get(0).get("genres");
					
					if (genresNode != null && genresNode.isArray()) {
						//System.out.println("GENRE NODE >>>>><<<<<<<<....... ARRAYYYYY >>>>>");
						String genres = "";
						Iterator<JsonNode> elems = genresNode.getElements();
						while (elems.hasNext()) {
							JsonNode genreNode = elems.next();
							genres += genreNode.getTextValue();
							if (elems.hasNext())
								genres += "|";
						}
						
						movieInfo.set(String.format("%s,%s,%s,%s", words[0], words[1], title, genres));
					} else {
						movieInfo.set(String.format("%s,%s,%s,%s", words[0], words[1], title, "Other"));
					}
				} else {
					movieInfo.set(String.format("%s,%s,%s,%s", words[0], words[1], title, "Other"));
				}
				
				count++;
				context.write(NullWritable.get(), movieInfo);
				
				//System.out.println(rootNode.size());
				//System.out.println(rootNode.getValueAsText());
				//System.out.println(rootNode.getTextValue())
				
				//String genre = genreNode.get(0).getTextValue();
				//System.out.println(genre);
//				java.util.Map<String, Object> result 
//					= mapper.readValue(url, new TypeReference<java.util.Map<String, Object>>() { });
				
			    //JSONBuilder rdr = Json.createReader(is);
//				if (result.containsKey("genres")) {
//					//List<String> genres = (List<String>) result.get("genres");
//					List<String> genres = new ArrayList<String>();
//					String csvGenre = "";
//					for (String genre : genres)
//						csvGenre += genre;
//					
//					System.out.println("Genres: " + result.get("genres"));
//					movieInfo.set(String.format("%s,%s,%s,%s", words[0], words[1], title, "genres"));
//				} else {
//					movieInfo.set(String.format("%s,%s,%s", words[0], words[1], title));
				//}

//				context.write(NullWritable.get(), movieInfo);
				
			} catch (Exception ex) {
				System.out.println("EXCEPTION>>>>> " + ex.getMessage());
			}
		}
	}

//		public static class Reduce extends
//		Reducer<Text, IntWritable, Set<String>, Collection<Double>> {
//
//			public void reduce(Set<String> user_pair, Iterable<Collection<Float>> ratings, Context context)
//					throws IOException, InterruptedException {
//				      
//			}
//		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = new Job(conf, "Dataset Merge");
			job.setJarByClass(DatasetMerge.class);
			
			job.setMapperClass(Map.class);
			//job.setCombinerClass(Reduce.class);
			//job.setReducerClass(Reduce.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);
		}
}