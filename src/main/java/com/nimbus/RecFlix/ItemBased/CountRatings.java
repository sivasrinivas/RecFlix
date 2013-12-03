package com.nimbus.RecFlix.ItemBased;


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
 * @author Gopikrishnan
 */
public class CountRatings {

        public static class Map extends Mapper<LongWritable, Text, Text, Text>{

                
                
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        String line = value.toString();
                        Text user_id = new Text();
                        Text item_id_rating = new Text();
                        
                        String[] parts = line.split(",");
                        String itemId =parts[1];
                        String userId = parts[0];
                        String rating = parts[2];
                        
                        item_id_rating.set(itemId+","+rating);
                        user_id.set(userId);
                        context.write(user_id, item_id_rating);
                }
        }
        
        public static class Reduce extends        Reducer<Text, Text, Text, Text> {
                
                public void reduce(Text userId, Iterable<Text> values, Context context)        throws IOException, InterruptedException {
                        StringBuilder builder = new StringBuilder();
                        int item_count = 0;
                        int item_sum = 0;
                        Text itemRating = new Text();
                        for(Text value : values){
                                //0-userId, 1-rating
                                System.out.println("Values: "+value);
                                String[] parts = value.toString().split(",");
                                item_count ++;
                                item_sum+=Integer.parseInt(parts[1]);
                                builder.append(parts[0]+","+parts[1]+" ");
                        }
                        //remove last extra space
                        builder.deleteCharAt(builder.length()-1);
                        
                        itemRating.set(item_count+","+item_sum+",("+builder.toString()+")");
                        context.write(userId, itemRating);
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
                Job job = new Job(conf, "count_ratings");
                job.setJarByClass(CountRatings.class);
                
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