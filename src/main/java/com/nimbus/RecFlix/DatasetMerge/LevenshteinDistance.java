/**
 * 
 */
package com.nimbus.RecFlix.DatasetMerge;

/**
 * @author prabal
 *
 */
public class LevenshteinDistance {
	public static int min(int a, int b, int c) {
		return Math.min(Math.min(a, b), c);
	}
	
	public static int compare(char[] s, char[] t) {
		
		int[][] dist = new int[s.length + 1][t.length + 1];
		
		// Source string can be modified to empty string by dropping all characters
		for (int i = 0; i <= s.length; i++) {
			dist[i][0] = i;
		}
		
		// Target string could be formed from empty source string by inserting
		// character by character
		for (int j = 1; j <= t.length; j++) {
			dist[0][j] = j;
		}
		
		// If the source character at i matches with target character at j
		//    then the cost is same as the previous operation (above-left) i.e., i-1 and j-1
		// Else calculate min of elements above + 1, left + 1, above-left (diagonal) + 1
		for (int i = 0; i <= s.length; i++) {
			for (int j = 0; j <= t.length; j++) {
				dist[i][j] = (s[i] == t[j]) 
					? dist[i - 1][j - 1] 
					: min(
						dist[i - 1][j] + 1, 
						dist[i][j - 1] + 1,
						dist[i - 1][j - 1] + 1);
			}
		}
		
		// The bottom and right most cell contains the net distance
		// to transform one string to another
		return dist[s.length][t.length];
	}

}
