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
	
	public static int computeDistance(char[] s, char[] t) {
		
		int[][] dist = new int[s.length + 1][t.length + 1];
		
		for (int i = 0; i <= s.length; i++) {
			dist[i][0] = i;
		}
		
		for (int j = 1; j <= t.length; j++) {
			dist[0][j] = j;
		}
		
		for (int i = 0; i <= s.length; i++) {
			for (int j = 0; j <= t.length; j++) {
				dist[i][j] = s[i] == t[j] 
					? dist[i - 1][j - 1] 
					: min(
						dist[i - 1][j] + 1, 
						dist[i][j - 1] + 1,
						dist[i - 1][j - 1] + 1);
			}
		}
		
		return dist[s.length][t.length];
	}

}
