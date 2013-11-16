package com.nimbus.RecFlix.UserBased;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserRanking {

	public static class Map extends Mapper<LongWritable, Text, MapKey4, MapValue4> {
        
        public void map(String user_pair, MapValue3 value, Context context)
                        throws IOException, InterruptedException {
                if(value.n > 0){
                	String[] s = user_pair.split(",");
                     //   Iterator<String> it  = item_keys.iterator();
                        context.write(new MapKey4(s[0], value.corr_sim /*value.cos_sim, value.reg_corr_sim, value.jaccard_sim*/), new MapValue4(s[1], value.n));
                }
        }
	}
	
	public static class Reduce extends
    Reducer<Text, IntWritable, Object, ReduceValue3> {
            public void reduce(MapKey4 key_sim, Iterable<MapValue4> values, Context context)
                            throws IOException, InterruptedException {
                                    for (MapValue4 value : values){
                                        context.write(null, new ReduceValue3(key_sim.item_name_x, value.item_name_y, key_sim.corr_sim, /*key_sim.cos_sim, key_sim.reg_corr_sim,
                                                        key_sim.jaccard_sim,*/ value.n));
                                    }

            }
    }
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Configuration conf = new Configuration();
        Job job = new Job(conf, "calculate_ranking");
        job.setJarByClass(CalculateRanking.class);
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
