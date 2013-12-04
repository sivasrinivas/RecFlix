package com.nimbus.RecFlix.UserBased;


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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.mahout.math.Arrays;


class UserRecommendation{
        public String userid;
        public MovieRatingPair[] movieid = new MovieRatingPair[10];
        public int recomCount = 0;
        public String[] genre = new String[3];
        public UserRecommendation(String userid) {
                this.userid = userid;
        }
        
        public void addMovieRatingPair(String movieid, double rating){
        	if(recomCount<10){
        		this.movieid[recomCount] = new MovieRatingPair(movieid, rating);
                recomCount++;
        	}
        }
        
        public boolean containsMovieId(String movieId){
        	for(int i=0; i<10; i++){
        		if(movieid[i]!=null && movieid[i].movieid.equals(movieId))
        			return true;
        	}
        	return false;
        }

		public void setMovieName(String movieId, String movieName) {
			for(int i=0; i<10; i++){
        		if(movieid[i]!=null && movieid[i].movieid.equals(movieId)){
        			movieid[i].movieName=movieName;
        		}
        	}
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

class UserMovieList{
	String userId;
	List<MovieRatingPair> movies;
	
	public UserMovieList(String userId){
		this.userId=userId;
		this.movies=new ArrayList<MovieRatingPair>();
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append(userId);
		sb.append(Arrays.toString(movies.toArray()));
		return sb.toString();
	}
}

class UserDetails{
    String userId;
    List<UserSimPAir> userSimPairList;
    UserDetails(String userId){
            userSimPairList = new ArrayList<UserSimPAir>();
            this.userId = userId;
    }

    public void insertUser(String user, double sim){
           userSimPairList.add(new UserSimPAir(user, sim));
    }
    
    public List<UserSimPAir> getIspairList(){
            return userSimPairList;
    }
    
}

class UserSimPAir{
        String user;
        double sim;
        UserSimPAir(String user, double sim){
                this.user = user;
                this.sim = sim;
        }
}

public class RecommendationManager{
        
        public static HashMap <String,UserDetails>  formMapFromResult() throws IOException{
                FileInputStream fstream1 = new FileInputStream("UserCoRelation.txt");
                DataInputStream in1 = new DataInputStream(fstream1);
                BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
                String strLine1;
                int c = 0;
                HashMap<String,UserDetails> h = new HashMap<String, UserDetails>();
                //strLine1 - userId1, userId2, similarity, movie count
                while ((strLine1 = br1.readLine()) != null)   {
                //	System.out.println(strLine1.split("	").length);								
                	if(strLine1.split("	").length > 1 && !strLine1.split("	")[1].split(",")[0].equalsIgnoreCase("NaN")){
         //       	System.out.println(strLine1.split("	")[1].split(",")[0]);
                	
                        String[] row = strLine1.split("	"); 
                        String[] users = row[0].split(",");
                        String userA = users[0], userB = users[1], similarity = row[1].split(",")[0];
                        double sim = Double.parseDouble(similarity);
                        
                        UserDetails userDet = null;
                        if(!h.containsKey(userA))
                                userDet = new UserDetails(userA);
                        else
                                userDet = h.get(userA);
                        userDet.insertUser(userB, sim);
                        h.put(userA,userDet);
                        if(!h.containsKey(userB))
                                userDet= new UserDetails(userB);
                        else
                                userDet = h.get(userB);
                        userDet.insertUser(userA, sim);
                        h.put(userB,userDet);
                	}
                }
                br1.close();
                return h;
        }
        
        
        public static UserRecommendation formRecommendations(String userId) throws IOException{
        	
        	FileInputStream fstream1 = new FileInputStream("SimUsersList.txt");
            DataInputStream in1 = new DataInputStream(fstream1);
            BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
            String strLine1;
    	
    		FileInputStream fstream2 = new FileInputStream("UserMovieList.txt");
            DataInputStream in2 = new DataInputStream(fstream2);
            BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));
            String strLine2;
    		
            FileInputStream fstream3 = new FileInputStream("MovieTitles.txt");
            DataInputStream in3 = new DataInputStream(fstream3);
            BufferedReader br3 = new BufferedReader(new InputStreamReader(in3));
            String strLine3;
            
            HashSet<String> recMovList = new HashSet<String>();
            HashMap<String, UserMovieList> similarUsers = new HashMap<String, UserMovieList>();
            UserMovieList currentUser = new UserMovieList(userId);
            
            UserRecommendation rec = new UserRecommendation(userId);
            
            String[] simUsers = new String[0];
                while ((strLine1 = br1.readLine()) != null)   {
                	
                	if(strLine1.contains(userId)){
                		String[] parts = strLine1.split(" ");
                    	if(parts.length>1){
                    		simUsers = parts[1].split("xxx");
                    	}
                    	for(String s : simUsers){
                    		String[] userSim = s.split(",");
                    		UserMovieList um = new UserMovieList(userSim[0]);
                    		similarUsers.put(userSim[0], um);
                    	}
                	}
                }
                
            while ((strLine2 = br2.readLine()) != null)   {
            	//userid, moviexxxrating...
            	String[] parts = strLine2.split("\t");
            	String[] movies = parts[1].split(",");
            	
            	if(parts[0].contains(currentUser.userId)){
            		for(String movie : movies){
            			String[] m = movie.split("xxx");
            			currentUser.movies.add(new MovieRatingPair(m[0], Double.parseDouble(m[1])));
            		}
            	}
            	else if(similarUsers.containsKey(parts[0]) ){
            		UserMovieList user = similarUsers.get(parts[0]);
            		for(String movie : movies){
            			String[] m = movie.split("xxx");
            			user.movies.add(new MovieRatingPair(m[0], Double.parseDouble(m[1])));
            		}
            	}
            }
            
            //get movies of A-B and add it to recommendation
            for(Entry<String, UserMovieList> b : similarUsers.entrySet()){
            	b.getValue().movies.removeAll(currentUser.movies);
            	for(MovieRatingPair mr : b.getValue().movies){
            		rec.addMovieRatingPair(mr.movieid, mr.rating);
            	}
            }
            
          	 while((strLine3 = br3.readLine())!=null){
          		 String[] parts = strLine3.split(",");
          		 if(rec.containsMovieId(parts[0])){
          			 rec.setMovieName(parts[0], parts[2]);
          		 }
          	 }
           	  br1.close();
           	  br2.close();
              br3.close();
            return rec;
        }
        
        public static void main(String []args) throws IOException{
        	BufferedWriter out = new BufferedWriter(new FileWriter("SimUsersList.txt"));
        	
                HashMap <String,UserDetails> resultMap = formMapFromResult(); //first function
                
                Iterator it = resultMap.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String,UserDetails> pairs = (Map.Entry)it.next();
                    UserDetails id = pairs.getValue();
                    out.write(pairs.getKey()+" ");
                 //   System.out.println(pairs.getKey() + " = ");
                    for(UserSimPAir is : id.userSimPairList){
                    	out.write(is.user+","+is.sim);
                    	out.write("xxx");
                    }
                    out.newLine();
                    it.remove(); // avoids a ConcurrentModificationException
                }
                out.close();
                
                String userId = "1109700";
                UserRecommendation result = formRecommendations(userId); //third function
                	MovieRatingPair[] mrp =  result.movieid;
                	System.out.println(result.recomCount);
                	for(int i = 0;i<result.recomCount;i++){
                		System.out.println(mrp[i].movieid+":"+mrp[i].movieName+":"+mrp[i].rating);
              	}
                	
                	
                
          //      ArrayList<Recommendation> resultFinal = setGenreInfo(result);

        }
}