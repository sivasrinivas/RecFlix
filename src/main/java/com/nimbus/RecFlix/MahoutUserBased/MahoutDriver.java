package com.nimbus.RecFlix.MahoutUserBased;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;


public class MahoutDriver {
	public static void main(String [] args){
		/*
		 * For all users in the file mentioned
		 * loop through the users and find recommendations
		 */
		try{
			Recommender mahoutRecommender = new MahoutUserBasedRecommender();
			parseAndRecommend(mahoutRecommender, "UserBasedOutput.txt");
			
		}
		catch(Exception e){
			System.out.println("Recommder has thrown taste exception or IOException"+ e);
		}
		

	}
	
	public static void parseAndRecommend(Recommender mahoutRecommender, String outputfilename){
		try{
		FileInputStream fstream = new FileInputStream("userlist100.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
		String line;
		PrintWriter writer = new PrintWriter(outputfilename, "UTF-8");
		long userID = 1;
		int noOfRecommendations = 10;
		while ((line = br.readLine()) != null) {
			try{
				userID = Long.parseLong(line);
			}
			catch(NumberFormatException ne){
				System.out.println("The userId should be long convertible"+ ne);
			}
			List<RecommendedItem> recommendedList = mahoutRecommender.recommend(userID, noOfRecommendations);
			StringBuilder recommendationList = new StringBuilder();
			recommendationList.append(userID);
			recommendationList.append("\t");
			recommendationList.append("[");
			if(recommendedList.isEmpty()){
				System.out.println("list is empty");
				
			}
			String delim = "";
			for(RecommendedItem rI: recommendedList){
				recommendationList.append(delim);
				recommendationList.append(rI.getItemID());
				recommendationList.append(":");
				recommendationList.append(rI.getValue());
				if(delim == "")
					delim = ",";

			}
			recommendationList.append("]");
			writer.println(recommendationList);
		}
		br.close();
		writer.close();
		}
		catch(IOException e){
			System.out.println("file exception" + e);
		}
		catch(TasteException te){
			System.out.println("taste exception" + te);
		}
	}

}

