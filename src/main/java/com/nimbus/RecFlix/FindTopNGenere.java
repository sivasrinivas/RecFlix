import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class FindTopNGenere {
	class Genere implements Comparable<Genere>{
		String genereName;
		double rating;
		int noOfMovies;
		double averageRating;
		public Genere(String genereName){
			this.genereName = genereName;
			
		}
		public void addRating(double rating){
			this.rating += rating;
			noOfMovies++;
		}
			
		
		public void  setAverageRating(){
			averageRating = rating/noOfMovies;
		}
		
		public int compareTo(Genere g){
			Double avRating = new Double(averageRating);
			Double gavRating = new Double(g.averageRating);
			
			return avRating.compareTo(gavRating);
			
		}
		
		@Override public boolean equals(Object o){
		    if (!(o instanceof Genere))
		        return false;
		    Genere g = (Genere) o;
		    return g.genereName.equals(genereName) ;
		}
			
		@Override public String toString() {
			StringBuilder str = new StringBuilder(genereName);
			str.append(":");
			str.append(averageRating);
			return str.toString();
		}

		
	}
	
//	public static ArrayList<Genere> getTopNGeners(ArrayList<Genere> genereList , int howMany){
//		ArrayList<Genere> topNGenere = new ArrayList<Genere>();
//		//check the arraylist size min is 3
//		for(int i = 0; i< howMany ; i++)
//		{
//			if(genereList.size() != 0){
//				Genere gmax = Collections.max(genereList);
//				genereList.remove(gmax);
//				topNGenere.add(gmax);						
//			}
//		}				
//		return topNGenere;
//	}
	String movieYearFile = "";
	String movieYearFileDelim = ",";
	String genererDelim = "\\|";
	int howManyGeneres;
	int howManyMovies;

	HashMap<String,HashMap<Genere,ArrayList<ItemRating>>> yearGenereMap = new HashMap<String,HashMap<Genere,ArrayList<ItemRating>>>();	
	HashMap<String, String>MovieIDNameMap = new HashMap<String, String>();
	static LinkedHashMap <String,ItemRating> avgRatingMap = new LinkedHashMap <String,ItemRating>();
	
	JSONArray jsonYearArray = new JSONArray();
	JSONObject jsonMainObj = new JSONObject();
	String jsonFilePath;
	
	static{
		try{
		avgRatingMap = temp.findAvgRatingForAll();
		
		}
		catch(IOException ioe){
			
		}
	}
	
  FindTopNGenere(String FullFilePath , String jsonFilePath , int howManyGeneres, int howManyMovies ){
	  this.howManyGeneres = howManyGeneres;
	  this.howManyMovies = howManyMovies;
	  movieYearFile = FullFilePath;
	  this.jsonFilePath = jsonFilePath;
	 jsonMainObj.put("name" ,"YearTopGenere" ); 
	 jsonYearArray = new JSONArray();	
	 if(FindTopNGenere.avgRatingMap == null){
		 System.out.println("no rating map");
	 }
		try{
		avgRatingMap = temp.findAvgRatingForAll();
		
		}
		catch(IOException ioe){
			
		}

		    Iterator<Entry<String, ItemRating>> it = avgRatingMap.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pairs = (Map.Entry)it.next();
		        System.out.println(pairs.getKey() + " = " + pairs.getValue());
		     //   it.remove(); // avoids a ConcurrentModificationException
		    }

  }
  public static void main(String [] args){

	 Driver(args);
	  
  }
  
	 

 public void readMovieYearFile(int howManyGeneres , int howManyMovies){
	 
	// read file..
	 BufferedReader in = null;
	try {
	    in = new BufferedReader(new FileReader(movieYearFile));
	    String str;
	    while ((str = in.readLine()) != null){
	    	
	    	// Step 1: split the info of MovieId,year,genereList
	    	String [] tokens = str.split(movieYearFileDelim);
	    	MovieIDNameMap.put(tokens[0],tokens[2]);
	    	// for testing added
//	    	for(String strr: tokens){
//	    		System.out.println(strr);
//	    	}
	    	HashMap<Genere,ArrayList<ItemRating>> genereMovieMap = null;
	    	
	    	// checking whether there is year in the first level of tree
	    	if(!yearGenereMap.containsKey(tokens[1])){
	    		genereMovieMap = new HashMap<Genere,ArrayList<ItemRating>>();
	     	} else{
	     		genereMovieMap = yearGenereMap.get(tokens[1]);
	     	}
	     	String [] genereList = tokens[3].split(genererDelim);
	     	// for testing added
//	     	for(String sttr: genereList){
//	     		System.out.println(sttr);
//	     	}
	    	
	    	
	    	if(genereList.length != 0){
	    		ArrayList<ItemRating> movieRatingListOfGenere;
	    		for(int i= 0 ; i< genereList.length ; i++){
	    			
	    			Genere g = new Genere(genereList[i]);
	    			//System.out.println(g.toString());
			    	if(!genereMovieMap.containsKey(g)){			    		
			    		movieRatingListOfGenere = new ArrayList<ItemRating>();
			    	} else {
			    		movieRatingListOfGenere = genereMovieMap.get(g);

			    	}
			    	ItemRating ir = new ItemRating(tokens[0]);
		    		movieRatingListOfGenere.add(ir);
		    		ItemRating  temp = avgRatingMap.get(tokens[0]);
		    		if(temp == null){
		    			System.out.println("no movie with movie ID" +tokens[0]);
		    			ir.avgRating = Double.valueOf(0.0);
		    			g.addRating(Double.valueOf(0.0));
		    			genereMovieMap.put(g,movieRatingListOfGenere);
		    			
		    		}
		    		else{
		    		ir.avgRating = temp.avgRating;
		    		g.addRating(Double.valueOf(avgRatingMap.get(tokens[0]).avgRating));
		    		genereMovieMap.put(g,movieRatingListOfGenere);
		    		}

	    		}
	    	
	    	} 
	    	yearGenereMap.put(tokens[1],genereMovieMap );

	    }
	}
	catch(IOException e){
		e.printStackTrace();
	}
	finally{
		try{
		    if (in != null) { 
		        in.close(); 
		    }
		}
		catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	// calculate the top rated Generes of a year
	
	for(Entry<String, HashMap<Genere, ArrayList<ItemRating>>> yearGenereEntry: yearGenereMap.entrySet()){
		HashMap<Genere,ArrayList<ItemRating>> genereMovieMap = yearGenereEntry.getValue();
		for(Entry<Genere, ArrayList<ItemRating>> genereEntry: genereMovieMap.entrySet()){
			Genere g = genereEntry.getKey();
			//System.out.println(g.toString());
			g.setAverageRating();
			
		}
		ArrayList<Genere> genereList = new ArrayList<Genere>();
		genereList.addAll(genereMovieMap.keySet());
		
		Collections.sort(genereList);
		JSONArray jsonGenereArray = new JSONArray();
		for(int i =0 ; i<howManyGeneres && i< genereList.size(); i++){
			ArrayList<ItemRating> irList = genereMovieMap.get(genereList.get(i));
			Collections.sort(irList);
			JSONArray jsonMovieArray = new JSONArray();
			int size = irList.size();
			for(int j = 0 ; j<howManyMovies && j< size ; j++){
				ItemRating temp = irList.get(j);
				JSONObject jratingob = new JSONObject();
				jratingob.put("name", "rating");
				jratingob.put("children" ,temp.avgRating);
				
				JSONArray jratArray = new JSONArray();
				jratArray.add(jratingob);
				JSONObject job = new JSONObject();
				job.put("name",MovieIDNameMap.get(temp.item) );
				job.put("children", jratArray);
				jsonMovieArray.add(job);
			}
			JSONObject jsonGenereMovieList = new JSONObject();
			jsonGenereMovieList.put("name", genereList.get(i).toString());
			//System.out.println(genereList.get(i).toString());
			jsonGenereMovieList.put("children", jsonMovieArray);
			jsonGenereArray.add(jsonGenereMovieList);
		}
		
		
		JSONObject jsonYearObject = new JSONObject();
		jsonYearObject.put("name" ,yearGenereEntry.getKey());
		jsonYearObject.put("children", jsonGenereArray);
		
		jsonYearArray.add(jsonYearObject);
		
	}
	 jsonMainObj.put("children", jsonYearArray);
	
 }
 
	 public void writeJsonFile(){
			try {
	
				FileWriter jsonFileWriter = new FileWriter(jsonFilePath);
				JSONArray jarray = new JSONArray();
				jarray.add(jsonMainObj);
				jsonFileWriter.write(jarray.toJSONString());
				jsonFileWriter.flush();
				jsonFileWriter.close();
	
			} catch (IOException e) {
				e.printStackTrace();
			}
	 }
	 
	 public static void Driver(String [] args){
		  int howManyGeneres = 3;
		  int howManyMovies = 3;
		  
		 if(args.length < 4){
			 if(args.length != 2){
				 System.out.println("Visualization: Check the usage");
				 usage();
				 return;
			 }
			 
		 } else if(args.length == 4){
			 howManyGeneres = Integer.parseInt(args[2]);
			 howManyMovies = Integer.parseInt(args[3]);
			 
		 } else{
			 System.out.println("Visualization: Check the usage");
			 usage();
			 return;
		 }

		 FindTopNGenere yearGenereVisualization = new  FindTopNGenere(args[0] , args[1], howManyGeneres, howManyMovies );
		 yearGenereVisualization.readMovieYearFile(howManyGeneres, howManyMovies);
		 yearGenereVisualization.writeJsonFile();
	 }
	 public static void usage(){
		 System.out.println("usage for calling FindTopNGenere: <MovieIdYearTitleGenereListFilePath> <ResultingJSONPath> " +
		 		"<howManyGenereDisplayed :default=3> <howManyMoviesDisplayed default = 3>");
		 
	 }
}
