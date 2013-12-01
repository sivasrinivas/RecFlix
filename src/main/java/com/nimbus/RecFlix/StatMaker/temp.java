package com.nimbus.RecFlix.StatMaker;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;


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
	MovieRatingPair(String movieid, double rating){
		this.movieid = movieid;
		this.rating = rating;
	}
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

class ItemRating{
	String item;
	Double ratingsum = 0.0;
	int count = 0;
	Double avgRating;
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

		HashMap<String,ItemDetails> h = new HashMap<String, ItemDetails>();
		while ((strLine1 = br1.readLine()) != null)   {
			String[] row = strLine1.split(","); 
			String item_A = row[0], item_B = row[1], similarity = row[2];
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
		br1.close();
		return h;
	}
	
	public static LinkedHashMap <String,ItemRating>  findAvgRatingForAll() throws IOException{
		FileInputStream fstream2 = new FileInputStream("ratings.txt");
		DataInputStream in2 = new DataInputStream(fstream2);
		BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
		String strLine2;

		LinkedHashMap<String,ItemRating> h2 = new LinkedHashMap<String, ItemRating>();
		while ((strLine2 = br2.readLine()) != null)   {

			String[] row = strLine2.split(","); 
			String  item_id= row[1], rating = row[2];
			Double dRating = Double.parseDouble(rating);
			ItemRating ir = null;
			if(!h2.containsKey(item_id))
				ir = new ItemRating();
			else
				ir = h2.get(item_id);
			ir.addRating(dRating);
			h2.put(item_id, ir);
		}
		br2.close();
		Iterator<String> mapIt = h2.keySet().iterator();
		while (mapIt.hasNext()){
			   String entry = mapIt.next();
			   
			   ItemRating value = h2.get(entry);
			   value.avgRating();
			   h2.put(entry, value);
			}
		return h2;
		
	}
	
	public static ArrayList<Recommendation> formRecommendations(HashMap <String,ItemDetails> resultMap,LinkedHashMap <String,ItemRating> 
	avgRatingMap) throws IOException{

		FileInputStream fstream3 = new FileInputStream("ratings.txt");
		DataInputStream in3 = new DataInputStream(fstream3);
		BufferedReader br3 = new BufferedReader(new InputStreamReader(in3));
		String strLine3;
		
		
		String user = null;
		int index = 0;
		boolean check = false;
		ArrayList<Recommendation> res = new ArrayList<Recommendation>();
		while ((strLine3 = br3.readLine()) != null)   {
			String[] row = strLine3.split(","); 
			String  userid= row[0], item = row[1];
			if(!check){
				user = userid;
				check = true;
				Recommendation rec = new Recommendation(userid);
				if(resultMap.containsKey(item)){
					ItemDetails itDetails = resultMap.get(item);
					ItemSimPAir isp = itDetails.getIspairList().get(0);
					if(avgRatingMap.containsKey(isp.item)){
						rec.addMovieRatingPair(isp.item, avgRatingMap.get(isp.item).avgRating);
					}
					
				}
				res.add(rec);
			}
			else{
				if(userid.equalsIgnoreCase(user)){
					Recommendation rec = res.get(index);
					if(resultMap.containsKey(item)){
						ItemDetails itDetails = resultMap.get(item);
						ItemSimPAir isp = itDetails.getIspairList().get(0);
						if(rec.recomCount <10)
							if(avgRatingMap.containsKey(isp.item)){
								rec.addMovieRatingPair(isp.item, avgRatingMap.get(isp.item).avgRating);
								res.set(index, rec);
							}
						
					}
					
				}
				else{
					user = userid;
					index++;
					Recommendation rec = new Recommendation(userid);
					if(resultMap.containsKey(item)){
						ItemDetails itDetails = resultMap.get(item);
						ItemSimPAir isp = itDetails.getIspairList().get(0);
						if(avgRatingMap.containsKey(isp.item)){
							rec.addMovieRatingPair(isp.item, avgRatingMap.get(isp.item).avgRating);
						}
					}
					res.add(rec);
				}
			}
			
		}
		br3.close();
		return res;
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
		
		HashMap <String,ItemDetails> resultMap = formMapFromResult();
		
		LinkedHashMap <String,ItemRating> avgRatingMap = findAvgRatingForAll();
		
		ArrayList<Recommendation> result = formRecommendations(resultMap,avgRatingMap);
		
		ArrayList<Recommendation> resultFinal = setGenreInfo(result);
		
	}
}
