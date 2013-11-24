/**
 * 
 */
package com.nimbus.RecFlix.UserBased;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeSet;

import org.apache.commons.math3.util.Decimal64;

/** This class reads input from user similarity file and initializes recommendation for each user
 * For handling all type of datasets, String datatype is used for all values
 * @author siva
 *
 */
public class RecommendationManager {
	HashMap<Integer, UserMovieList> userMovieBase;
	HashMap<Integer, SimilarUsers> similarUserBase;
	public RecommendationManager(){
		userMovieBase = new HashMap<Integer, UserMovieList>();
		similarUserBase = new HashMap<Integer, SimilarUsers>();
	}
	
	//read data from user pair, similarity file and loads similarUserBase with userid and top10 similar users
	public void loadSimilarUsersBase(String filePath){
		 try {
			BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
			String input; //input formar user1, user2, similarity
			while((input=br.readLine()) != null){
				String[] parts = input.split(",");
				int userId1 = Integer.parseInt(parts[0]);
				int userId2 = Integer.parseInt(parts[1]);
				double similarity = Double.parseDouble(parts[2]);
				UserSimilarityPair us = new UserSimilarityPair(userId2, similarity);
				//check userid already exists in similarUserBase or not
				if(similarUserBase.containsKey(userId1)){
					SimilarUsers su = similarUserBase.get(userId1);
					su.addUser(us);
				}else{
					SimilarUsers su = new SimilarUsers(userId1);
					su.addUser(us);
					similarUserBase.put(userId1, su);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
	}
	//reads data from file and loads UserMovieBase with userid and their movie list
	public void loadUserMovieList(String filePath){
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
			String input; //input format userid, movie1,rating1 movie2,rating2 
			while((input=br.readLine())!=null){
				
				int userId = Integer.parseInt(input.substring(0,input.indexOf(",")));
				String[] movieList = input.substring(input.indexOf(",")+1).split(" ");
				List<MovieRatingPair> list = new ArrayList<MovieRatingPair>();
				for(String movie : movieList){
					MovieRatingPair mvp = new MovieRatingPair(movie);
					list.add(mvp);
				}
				
				UserMovieList uml = new UserMovieList(userId, list);
				userMovieBase.put(userId, uml);
			}
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void generateRecommendations(String outputPath){
		try {
			BufferedWriter br = new BufferedWriter(new FileWriter(new File(outputPath)));
			for(Integer userId :userMovieBase.keySet()){
				List<MovieRatingPair> movieList1 = userMovieBase.get(userId).movieList;
				SimilarUsers su = similarUserBase.get(userId);
				TreeSet<MovieRatingPair> set = new TreeSet<MovieRatingPair>();
				
				UserSimilarityPair similarUser= su.getNextSimilarUser();
				while(set.size()<10 && similarUser!=null){
					List<MovieRatingPair> movieList2 = userMovieBase.get(similarUser.userId).movieList;
					List<MovieRatingPair> duplicateList = new ArrayList<MovieRatingPair>();
					duplicateList.addAll(movieList2);
					duplicateList.removeAll(movieList1);
					set.addAll(duplicateList);
					//get next user
					similarUser = su.getNextSimilarUser();
				}
				
				Recommendation r = new Recommendation(userId, set);
				//add recommednation for userid to file
				br.write(r.toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args){
		
		RecommendationManager rm = new RecommendationManager();
		rm.loadUserMovieList("usermovielist");
		rm.loadSimilarUsersBase("");
		rm.generateRecommendations("");
	}
}

class UserMovieList{
	int userId;
	List<MovieRatingPair> movieList;
	public UserMovieList(int userId, List<MovieRatingPair> movieList){
		this.userId=userId;
		this.movieList=movieList;
	}
}
/**
 * This class contaise top10 more similar users list
 * @author siva
 *
 */
class SimilarUsers{
	int userId;
	int limit = 10;
	PriorityQueue<UserSimilarityPair> simlarUsers;
	public SimilarUsers(int userId){
		this.userId=userId;
		this.simlarUsers=new PriorityQueue<UserSimilarityPair>(10, UserSimilarityPair.similarityComp);
	}
	
	public void addUser(UserSimilarityPair newUser){
		if(this.simlarUsers.size()<10){
			this.simlarUsers.add(newUser);
		}else{
			if(Double.compare(newUser.similarity, this.simlarUsers.peek().similarity)>0){
				this.simlarUsers.remove();
				this.simlarUsers.add(newUser);
			}
		}
	}
	
	public UserSimilarityPair getNextSimilarUser(){
		return this.simlarUsers.remove();
	}
	
}

class UserSimilarityPair{
	int userId;
	double similarity;
	public static Comparator<UserSimilarityPair> similarityComp = new Comparator<UserSimilarityPair>() {
		
		public int compare(UserSimilarityPair o1, UserSimilarityPair o2) {
			return Double.compare(o1.similarity, o2.similarity);
		}
	};
		
	
	public UserSimilarityPair(int userId, double similarity){
		this.userId=userId;
		this.similarity=similarity;
	}
	
	public UserSimilarityPair(String userSimilarity){
		String[] parts = userSimilarity.split(",");
		this.userId=Integer.parseInt(parts[0]);
		this.similarity=Double.parseDouble(parts[1]);
	}
}
class MovieRatingPair{
	int movieId;
	double rating;
	
	public MovieRatingPair(int movieId, double rating){
		this.movieId=movieId;
		this.rating=rating;
	}
	
	public MovieRatingPair(String moiveRating){
		String[] parts = moiveRating.split(",");
		this.movieId=Integer.parseInt(parts[0]);
		this.rating=Double.parseDouble(parts[1]);
	}
}

class Recommendation{
	int userId;
	TreeSet<MovieRatingPair> recommendationSet;
	public Recommendation(int userId, TreeSet<MovieRatingPair> recommendationSet){
		this.userId=userId;
		this.recommendationSet = recommendationSet;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		DecimalFormat format = new DecimalFormat("0.00");
		
		sb.append(userId+"\t");
		for(MovieRatingPair movie : recommendationSet){
			sb.append(movie.movieId+","+format.format(movie.rating)+" ");
		}
		return sb.toString();
	}
}