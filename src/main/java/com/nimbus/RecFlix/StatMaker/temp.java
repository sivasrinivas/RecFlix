package com.nimbus.RecFlix.StatMaker;





import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;


class Recommendation{
        public String userid;
        public MovieRatingPair[] movieid = new MovieRatingPair[10];
        public int recomCount = 0;
        public String[] genre = new String[3];
        public Recommendation(String userid) {
                this.userid = userid;
        }
        
        public void addMovieRatingPair(String movieid, double rating){
                this.movieid[recomCount] = new MovieRatingPair(movieid, rating);
                recomCount++;
        }
}

class MovieRatingPair{
        String movieid;
        double rating;
        public String movieName = null;
        MovieRatingPair(String movieid, double rating){
                this.movieid = movieid;
                this.rating = rating;
        }
      //Added by Divya
    	@Override public String toString() {
    		StringBuilder str = new StringBuilder(movieid);
    		str.append(":");
    		str.append(rating);
    		return str.toString();
    	}
    	//Addition Done
}

class ItemDetails{
        String item;
        List<ItemSimPAir> ispairList;
        ItemDetails(String item){
                ispairList = new ArrayList<ItemSimPAir>();
                this.item = item;
        }

        public void insertItem(String item, double sim){
                ispairList.add(new ItemSimPAir(item, sim));
        }
        
        public List<ItemSimPAir> getIspairList(){
                return ispairList;
        }
        
}

class ItemRating implements Comparable<ItemRating>{
        String item;
        public String itemname = null;
        Double ratingsum = 0.0;
        int count = 0;
        Double avgRating;
      //Added by Divya
    	ItemRating(String item){
    		this.item = item;
    	}
    	// Addition Done
        public void addRating(Double rating){
                ratingsum += rating;
                count++;
        }
        public void  avgRating(){
                avgRating = ratingsum/count;
        }
        
        public double getAvgRating(){
                return avgRating;
        }
     // Added by Divya
    	public int compareTo(ItemRating ir){
    		Double avRatingObj = new Double(avgRating);
    		Double gavRatingObj = new Double(ir.avgRating);
    		
    		return avRatingObj.compareTo(gavRatingObj);
    		
    	}
    	
    	@Override public String toString() {
    		StringBuilder str = new StringBuilder(item);
    		str.append(":");
    		str.append(avgRating.doubleValue());
    		return str.toString();
    	}
    	// Addition Done
}

class ItemSimPAir{
        String item;
        double sim;
        ItemSimPAir(String item, double sim){
                this.item = item;
                this.sim = sim;
        }
}

public class temp {
        
        public static HashMap <String,ItemDetails>  formMapFromResult() throws IOException{
                FileInputStream fstream1 = new FileInputStream("result.txt");
                DataInputStream in1 = new DataInputStream(fstream1);
                BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
                String strLine1;
int c = 0;
                HashMap<String,ItemDetails> h = new HashMap<String, ItemDetails>();
                while ((strLine1 = br1.readLine()) != null)   {
                //	System.out.println(strLine1.split("	").length);								
                	if(strLine1.split("	").length > 1 && !strLine1.split("	")[1].split(",")[0].equalsIgnoreCase("NaN")){
         //       	System.out.println(strLine1.split("	")[1].split(",")[0]);
                	
                        String[] row = strLine1.split("	"); 
                        String[] items = row[0].split(",");
                        String item_A = items[0], item_B = items[1], similarity = row[1].split(",")[0];
                        double sim = Double.parseDouble(similarity);
                        ItemDetails it_det = null;
                        if(!h.containsKey(item_A))
                                it_det= new ItemDetails(item_A);
                        else
                                it_det = h.get(item_A);
                        it_det.insertItem(item_B, sim);
                        h.put(item_A,it_det);
                        if(!h.containsKey(item_B))
                                it_det= new ItemDetails(item_B);
                        else
                                it_det = h.get(item_B);
                        it_det.insertItem(item_A, sim);
                        h.put(item_B,it_det);
                	}
                }
                br1.close();
                return h;
        }
        
        public static LinkedHashMap <String,ItemRating>  findAvgRatingForAll() throws IOException{
        	BufferedWriter out = new BufferedWriter(new FileWriter("AvgRatingsList.txt"));
        	
                FileInputStream fstream2 = new FileInputStream("ratings.txt");
                DataInputStream in2 = new DataInputStream(fstream2);
                BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
                String strLine2;
                
                FileInputStream fstream4 = new FileInputStream("movie_titles.txt");
                DataInputStream in4 = new DataInputStream(fstream4);
                BufferedReader br4 = new BufferedReader(new InputStreamReader(in4));
                String strLine4;

                LinkedHashMap<String,ItemRating> h2 = new LinkedHashMap<String, ItemRating>();
                while ((strLine2 = br2.readLine()) != null)   {
                        String[] row = strLine2.split(","); 
                        String  item_id= row[1], rating = row[2];
                        Double dRating = Double.parseDouble(rating);
                        ItemRating ir = null;
                        if(!h2.containsKey(item_id))
                                ir = new ItemRating(item_id);
                        else
                                ir = h2.get(item_id);
                        ir.addRating(dRating);
                        h2.put(item_id, ir);
                }
                br2.close();
                while ((strLine4 = br4.readLine()) != null)   {
            		  String[] row = strLine4.split(",");
            		  if(h2.containsKey(row[0])){
//            			/  MovieRatingPair mp = new MovieRatingPair(row[0], Double.parseDouble(row[1]));
            			ItemRating ir = h2.get(row[0]);
            			ir.itemname = row[2];
            			h2.put(row[0], ir);
            			//  index1++;
            		  }
            	  }

                br4.close();
                Iterator<String> mapIt = h2.keySet().iterator();
                int c  = 0;
                while (mapIt.hasNext()){
                           String entry = mapIt.next();
                           c++;
                           ItemRating value = h2.get(entry);
                           value.avgRating();
                           out.write(entry+","+value.itemname+","+value.avgRating);
                           out.newLine();
               //            System.out.println(entry+","+value.count+","+value.avgRating+","+c);
                           h2.put(entry, value);
                        }
          	 
                out.close();
                return h2;
                
        }
        
        public static Recommendation formRecommendations(String userId) throws IOException{
        	
        	FileInputStream fstream1 = new FileInputStream("ratings.txt");
            DataInputStream in1 = new DataInputStream(fstream1);
            BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
            String strLine1;
        	
        		
        	
        		FileInputStream fstream2 = new FileInputStream("SimMovsList.txt");
                DataInputStream in2 = new DataInputStream(fstream2);
                BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
                String strLine2;
        		
                FileInputStream fstream3 = new FileInputStream("AvgRatingsList.txt");
                DataInputStream in3 = new DataInputStream(fstream3);
                BufferedReader br3 = new BufferedReader(new InputStreamReader(in3));
                String strLine3;
                
                
                
                int recCount = 0;
                HashSet<String> recMovList = new HashSet<String>();
                HashSet<String> watchedMovList = new HashSet<String>();
                TreeMap<Double,ArrayList<String>> candidateMovList = new TreeMap<Double,ArrayList<String>>();
                Recommendation rec = new Recommendation(userId);
                
                while ((strLine1 = br1.readLine()) != null)   {
                	String[] row = strLine1.split((","));
                	if(row[0].equalsIgnoreCase(userId)){
                		if(!watchedMovList.contains(row[1])){
                			watchedMovList.add(row[1]);
                		}
                	}
                }
                
                
                while ((strLine2 = br2.readLine()) != null)   {
                	String[] row = strLine2.split(" ");
                //	System.out.println(row[0]);
                	if(watchedMovList.contains(row[0])){
                	//	System.out.println(row[1]);
                		String[] movSimPairs = row[1].split("xxx");
                //		System.out.println(movSimPairs.length);
                		for(String movSimPair : movSimPairs){
                		//	System.out.println(movSimPair);
                			String[] pairArr = movSimPair.split(",");
                			Double sim = Double.parseDouble(pairArr[1]);
                			String movId = pairArr[0];
                			ArrayList<String> movList = null;
                			if(candidateMovList.containsKey(sim)){
                				movList = candidateMovList.get(sim);
                				
                			}
                			else{
                				movList = new ArrayList<String>();
                				
                			}
                			movList.add(movId);
            				candidateMovList.put(sim, movList);
                		}
                	}
                }
                
                for(Map.Entry<Double,ArrayList<String>> entry : candidateMovList.entrySet()) {
                	  Double key = entry.getKey();
                	  ArrayList<String> movList1 = entry.getValue();
                	  
                	  for(String movie : movList1){
                		  if(recCount == 10)
                			  break;
                		  if(!recMovList.contains(movie)){
                		  recMovList.add(movie);
                		  recCount++;
                	  }
                	
                		  if(recCount == 10)
                			  break;
                	}
                }
                int index = 0;
                	  while ((strLine3 = br3.readLine()) != null)   {
                		  String[] row = strLine3.split(",");
                		  if(recMovList.contains(row[0])){
                			  MovieRatingPair mp = new MovieRatingPair(row[0], Double.parseDouble(row[2]));
                			  mp.movieName = row[1];
                			  rec.movieid[index] = mp;
                			  index++;
                		  }
                		  
                	  }
                	  
                	  
                	  rec.recomCount = index;
                	  br1.close();
                	  br2.close();
                br3.close();
                return rec;
        }
        
        public static ArrayList<Recommendation> setGenreInfo(ArrayList<Recommendation> result) throws IOException {
                /* Genre info*/
                FileInputStream fstream4 = new FileInputStream("genres.txt");
                DataInputStream in4 = new DataInputStream(fstream4);
                BufferedReader br4 = new BufferedReader(new InputStreamReader(in4));
                String strLine4;
                HashMap<String,ArrayList<String>> h3 = new HashMap<String, ArrayList<String>>();
                while ((strLine4 = br4.readLine()) != null)   {
                        String[] row = strLine4.split(","); 
                        String  movieid= row[0], genres = row[2] ;
                        String[] genre = genres.split(" ");
                        ArrayList<String> a = new ArrayList<String>();
                        for(int i=0;i<genre.length;i++){
                                a.add(genre[i]);
                        }
                        h3.put(movieid, a);
                }
                br4.close();
                
                for(int i =0;i<result.size();i++){
                        Recommendation rec = result.get(i);
                        HashMap<String,Integer> commonGenres = new HashMap<String, Integer>();
                        for(int j=0;j<rec.recomCount;i++){
                                if(h3.containsKey(rec.movieid[j].movieid)){
                                        
                                        ArrayList<String> l = h3.get(rec.movieid[j].movieid);
                                        for(int k =0;k<l.size();k++){
                                                if(!commonGenres.containsKey(l.get(k))){
                                                        commonGenres.put(l.get(k),1);
                                                }
                                                else{
                                                        int temp = commonGenres.get(l.get(k));
                                                        temp++;
                                                        commonGenres.put(l.get(k),temp);
                                                }
                                        }
                                }
                        }
                        
                        for(int l =0;l<3;l++){
                                Iterator<String> s = commonGenres.keySet().iterator();
                                int max = commonGenres.get(s.next());
                                String mxs = s.next();
                                while (s.hasNext()){
                                           String entry = s.next();
                                           if(commonGenres.get(entry) > max){
                                                   max = commonGenres.get(entry);
                                                   mxs = entry;
                                           }
                                }
                                rec.genre[l] = mxs;
                                commonGenres.remove(mxs);
                        }
                        
                        result.set(i, rec);
                }
                return result;
        }
        
        
        public static void main(String []args) throws IOException{
        	BufferedWriter out = new BufferedWriter(new FileWriter("SimMovsList.txt"));
        	
                HashMap <String,ItemDetails> resultMap = formMapFromResult();
                
                Iterator it = resultMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String,ItemDetails> pairs = (Map.Entry)it.next();
                    ItemDetails id = pairs.getValue();
                    out.write(pairs.getKey()+" ");
                 //   System.out.println(pairs.getKey() + " = ");
                    for(ItemSimPAir is : id.ispairList){
                    	out.write(is.item+","+is.sim);
                    	out.write("xxx");
                    }
                    out.newLine();
                    it.remove(); // avoids a ConcurrentModificationException
                }
                out.close();
                LinkedHashMap <String,ItemRating> avgRatingMap = findAvgRatingForAll();
                String userId = "1025579";
                Recommendation result = formRecommendations(userId);
                	MovieRatingPair[] mrp =  result.movieid;
                	System.out.println(result.recomCount);
                	for(int i = 0;i<result.recomCount;i++){
                		System.out.println(mrp[i].movieid+":"+mrp[i].movieName+":"+mrp[i].rating);
                	}
                	
                	
                
          //      ArrayList<Recommendation> resultFinal = setGenreInfo(result);

        }
}
