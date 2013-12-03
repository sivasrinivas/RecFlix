package com.nimbus.RecFlix.ItemBased;

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

import com.nimbus.RecFlix.ItemBased.ItemRatings.Map;
import com.nimbus.RecFlix.ItemBased.ItemRatings.Reduce;

class MapperValueRatings{
        int itemCount;
        int itemRatingSum;
        HashMap<Integer, Float> ratings;
        
        public MapperValueRatings(String input){
                System.out.println(input);
                itemCount=0;
                itemRatingSum=0;
                ratings = new HashMap<Integer, Float>();
        }
        
}

class ReducerValueSimilarity{
        float similarity;
        public ReducerValueSimilarity(){
                similarity = 0.0f;
        }
}

class ItemRating{
        String itemId;
        double rating;
}

class ItemPair{
        String itemId1;
        String itemId2;
        double rating1;
        double rating2;
        public ItemPair(String itemId1, String itemId2, double rating1, double rating2){
                this.itemId1 = itemId1;
                this.itemId2 = itemId2;
        }
        
        public ItemPair(){
                itemId1=null;
                itemId2=null;
        }

        /* (non-Javadoc)
         * @see java.lang.Object#hashCode()
         */
      /*  @Override
        public int hashCode() {
                final int prime = 31;
                int result = 1;
                result = prime * result + (int) (userId1 ^ (userId1 >>> 32));
                result = prime * result + (int) (userId2 ^ (userId2 >>> 32));
                return result;
        }*/

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
                if (!(obj instanceof ItemPair)) {
                        return false;
                }
                ItemPair other = (ItemPair) obj;
                if (itemId1 != other.itemId1) {
                        return false;
                }
                if (itemId2 != other.itemId2) {
                        return false;
                }
                return true;
        }
        
        
}

public class CalculateSimilarity {
        
        //combinations
        public static List<List<String>> lists = new ArrayList<List<String>>();

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

                public void map(LongWritable itemId, Text value, Context context)        throws IOException, InterruptedException {
                        Text itemPair = new Text();
                        Text ratingPair = new Text();
                        
                        try{
                                //userId, itemCount, itemRatingSum,(itemId, rating,...)
                                System.out.println("values---->"+value);
                                String line = value.toString();
                                String itemCount = line.substring(0, line.indexOf(","));
                                String itemRatingSum = line.substring(line.indexOf(",")+1,line.indexOf(",(") );
                                String[] ratings = line.substring(line.indexOf(",(")+2, line.indexOf(")")).split(" ");
                                //get combinations for array of (itemId,rating)
                                if(ratings.length>1){
                                        combination(ratings); /*combinations will be set in lists variable*/
                                        System.out.println(Arrays.toString(lists.toArray()));
                                        for (List<String> combi : lists){
                                                String[] s1 = combi.get(0).split(",");
                                                String[] s2 = combi.get(1).split(",");
                                                itemPair.set(s1[0]+","+s2[0]);
                                                ratingPair.set(s1[1]+","+s2[1]);
                                                //writing to context userPair and their ratings pair
                                                context.write(itemPair, ratingPair);
                                        }
                                        lists = new ArrayList<List<String>>();
                                }
                        }catch(Exception e){
                                System.out.println("Got exception :)");
                        }
                        
                }
        }
        
        
        public static class Reduce extends Reducer<Text, Text, Text, Text>{
                public void reduce(Text itemPair, Iterable<Text> ratings, Context context)        throws IOException, InterruptedException {
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
                                
                                
                                double numerator = count*sum_xy - sum_x*sum_y;
                                double denominator = Math.sqrt(count*sum_xx - sum_x*sum_x)*Math.sqrt(count*sum_yy - sum_y*sum_y);
                                double corr_sim = numerator/denominator;
                                
                                similarity.set(corr_sim+","+count);
                                context.write(itemPair, similarity);
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
                Job job = new Job(conf, "calculate_similarity");
                job.setJarByClass(CalculateSimilarity.class);
                
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