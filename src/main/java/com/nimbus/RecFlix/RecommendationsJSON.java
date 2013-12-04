import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

//import org.apache.mahout.cf.taste.recommender.Recommender;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

//import com.nimbus.RecFlix.UserBased.RecommendationManager;


public class RecommendationsJSON {

	
	 public static void Driver(String [] args){

		 String mahoutFileName = "UserBasedOutputnew.txt";
		 boolean userBased = Boolean.parseBoolean(args[2]);
		 boolean itemBased = Boolean.parseBoolean(args[3]);

		 String userId = "214144";
		 createJsonForUserBased(userId);
		 createJsonForItemBased(userId);
		 
	 }
	 
	 public static void createJsonFromMahout(String userId, boolean userbased){
		 String UserBasedFileDir = "MahoutUserBasedRec";
		 String ItemBasedFileDir = "MahoutItemBasedRec";
		 File dir = null;
		 FileInputStream fstream = null ;
		 String line;
		 BufferedReader br = null;
		 String fileName = "userJson.json";
			
				try{
					if(userbased){
						dir = new File(UserBasedFileDir);

					}
					else{
					   dir = new File(ItemBasedFileDir);


					}
					
	                FileInputStream fstream2 = new FileInputStream("movie_titles.txt");
	                BufferedReader brMovieTitles = new BufferedReader(new InputStreamReader(fstream2));
					String[] children = dir.list();
					  for (int i=0; i<children.length; i++) {
					        // Get filename of file or directory
							fstream = new FileInputStream(children[i]);
							br = new BufferedReader(new InputStreamReader(fstream));
					   
						while ((line = br.readLine()) != null) {
							String [] tokens = line.split("\\t");
							if(tokens[0].equals(userId)){
								tokens[1].replace("[" ,"");
								tokens[1].replace("]" ,"");
								String [] movieIdRating = tokens[1].split(",");
			
								JSONArray jsonRecArray = new JSONArray();
								for(int j= 0 ; j<movieIdRating.length; j++){
									// adding last level of hierarchy that is rating
									
									// processing to get movie title from name
									String[] movieidratingTokens = movieIdRating[j].split(":");
									String movieName = "";
									while ((line = br.readLine()) != null) {
										String [] movietitles = line.split(",");
										if(movieidratingTokens[0].equals(movietitles[0]))
											movieName = movietitles[2];
									}
									
									JSONObject jsonRatingObj = new JSONObject();
									jsonRatingObj.put("name" ,movieidratingTokens[1]);
									JSONArray jsonRatingArray = new JSONArray();
									jsonRatingArray.add(jsonRatingObj);
									
									JSONObject jsonMovieObj = new JSONObject();
									jsonMovieObj.put("name",movieName);
									jsonMovieObj.put("children",jsonRatingArray );
									jsonRecArray.add(jsonMovieObj);
								}
								JSONObject jsonUserObj = new JSONObject();
								jsonUserObj.put("name", userId );
								jsonUserObj.put("children", jsonRecArray);
								
								writeJsonFile(fileName.toString(),jsonUserObj);
								
							}
							
							
						}
					  }
				} catch(IOException e){
					
				}
		 
	 }
	public static void createJsonForItemBased(String userId){
		Recommendation result = null;
		StringBuilder fileName = new StringBuilder("userJSON.json");
		try{
			result = temp.formRecommendations(userId);	

	} catch(IOException ioe){
		
	}

		if(result == null)
			return;
			
			JSONArray jsonRecArray = new JSONArray();

			for(int j= 0 ; j<result.recomCount; j++){
				// adding last level of hierarchy that is rating
				JSONObject jsonRatingObj = new JSONObject();
				jsonRatingObj.put("name" ,result.movieid[j].rating );
				//jsonRatingObj.put("name" ,"10");
				JSONArray jsonRatingArray = new JSONArray();
				jsonRatingArray.add(jsonRatingObj);
				
				JSONObject jsonMovieObj = new JSONObject();
				jsonMovieObj.put("name",result.movieid[j].movieName);
				//jsonMovieObj.put("name","100");
				jsonMovieObj.put("children",jsonRatingArray );
				jsonRecArray.add(jsonMovieObj);
			}
			JSONObject jsonUserObj = new JSONObject();
			jsonUserObj.put("name", result.userid );
			jsonUserObj.put("children", jsonRecArray);
			
			writeJsonFile(fileName.toString(),jsonUserObj);
			
		
	}
	public static void createJsonForUserBased(String userId){
		UserRecommendation result = null;
		StringBuilder fileName = new StringBuilder("userJSON.json");
		try{

			result = RecommendationManager.formRecommendations(userId);	

	} catch(IOException ioe){
		
	}

		if(result == null)
			return;
			
			JSONArray jsonRecArray = new JSONArray();

			for(int j= 0 ; j<result.recomCount; j++){
				// adding last level of hierarchy that is rating
				JSONObject jsonRatingObj = new JSONObject();
				jsonRatingObj.put("name" ,result.movieid[j].rating );
				//jsonRatingObj.put("name" ,"10");
				JSONArray jsonRatingArray = new JSONArray();
				jsonRatingArray.add(jsonRatingObj);
				
				JSONObject jsonMovieObj = new JSONObject();
				jsonMovieObj.put("name",result.movieid[j].movieName);
				//jsonMovieObj.put("name","100");
				jsonMovieObj.put("children",jsonRatingArray );
				jsonRecArray.add(jsonMovieObj);
			}
			JSONObject jsonUserObj = new JSONObject();
			jsonUserObj.put("name", result.userid );
			jsonUserObj.put("children", jsonRecArray);
			
			writeJsonFile(fileName.toString(),jsonUserObj);
			
		
	}
	
	 public static void writeJsonFile(String jsonFileFullPath, JSONObject jsonUserObj ){
			try {
	
				FileWriter jsonFileWriter = new FileWriter(jsonFileFullPath);
				jsonFileWriter.write(jsonUserObj.toJSONString());
				jsonFileWriter.flush();
				jsonFileWriter.close();
	
			} catch (IOException e) {
				e.printStackTrace();
			}
	 }
	 public static void usage(){
		 System.out.println("usage for calling RecommendationJSON:  userID <ResultingJSONDirPath> " +
		 		"true/false (this represents userBased) true/false(this represents ItemBasedFlag)");
		 
	 }
	 public static void main(String[] args){
		 Driver(args);
	 }

}
