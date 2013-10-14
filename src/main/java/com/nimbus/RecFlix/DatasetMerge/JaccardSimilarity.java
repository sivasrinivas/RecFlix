/**
 * 
 */
package com.nimbus.RecFlix.DatasetMerge;

import java.util.HashSet;
import java.util.Set;

/**
 * @author prabal
 *
 */
public class JaccardSimilarity {
	
	// N-Grams or K-Shingles
	public static final int K = 3;
	
	// Create nGrams or kShingles
	public Set<String> makeKShingles(String s, int k) {
		Set<String> shingles = new HashSet<String>();
		
		for (int i = 0; i < s.length() - k + 1; i++)
			shingles.add(s.substring(i, i + k));
		
		return shingles;
	}
	
	// Calculate the similarity
	public double compare(String s, String t) {
		if (s.equals(t))
			return 1.0;
		
		Set<String> sShingles = makeKShingles(s, K);
		Set<String> tShingles = makeKShingles(t, K);
		
		if (sShingles.isEmpty() || tShingles.isEmpty())
			return 0.0;
		
		int intersection = 0;
		for (String shingle : sShingles)
			if (tShingles.contains(shingle))
				intersection++;
		
		return (double)intersection/(double)Math.min(sShingles.size(), tShingles.size());
	}
}
