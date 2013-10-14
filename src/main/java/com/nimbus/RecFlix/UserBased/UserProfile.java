package com.nimbus.RecFlix.UserBased;

import java.util.List;

public class UserProfile {

	/**
	 * @param args
	 * @author Siva
	 */
	
	int id;
	List<MovieRating> movieList;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public List<MovieRating> getMovieList() {
		return movieList;
	}
	public void setMovieList(List<MovieRating> movieList) {
		this.movieList = movieList;
	}
}

class MovieRating{
	String name;
	int rating;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getRating() {
		return rating;
	}
	public void setRating(int rating) {
		this.rating = rating;
	}
	
}