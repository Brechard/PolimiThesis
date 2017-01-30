package com.polimi.thesis;

public class SearchHelper {

	public static int searchEndCurlyBracket(int start, String block){
		int i = start;
		int par = 0;
		while (i < block.length()) {
			if(block.charAt(i)=='{')
				par++;
			else if  (block.charAt(i)=='}'){
				par--;
				if (par == 0)
					return i;
			}
			i++;
		}
		return i;
	}
	
	public static int searchEndParenthesis(int start, String block){
		int i = start;
		int par = 0;
		while (i < block.length()) {
			if(block.charAt(i)=='(')
				par++;
			else if  (block.charAt(i)==')'){
				par--;
				if (par == 0)
					return i;
			} else if(block.charAt(i) == ';' && par == 0)
				return i;
			i++;
		}
		return i;
	}
	
	/*
	 * This method searchs the end of a block of code given the begining 
	 */
	public static int searchEndBlock(int start, int par2, String block){
//		System.out.println("start at: " +start);
		int i = searchEndParenthesis(start, block);
		if(block.charAt(i) == ')')
			par2++;
		int par = par2;
		if (i == start) {
			return i;
		}
		while (i < block.length()) {
			if(block.charAt(i) == '('){
				i = searchEndBlock(i, par++, block); // If we find another opening parenthesis we have to reach the end of it again
				break;
			} else if (block.charAt(i) == ')')
				par --;
			else if  (block.charAt(i)==';' && par == 0)
				return i;
			i++;
		}		
		return i;
	}
	
	/*
	 * This method searchs the start of a block of code surrounded of parenthesis
	 */
	public static int searchStartParenthesisBlock(String block, int end){
//		System.out.println("start at: " +start);
		int i = end;
		int par = 0;

		while (i > 0) {
			if(block.charAt(i)==')') par++;                              
			else if  (block.charAt(i)=='('){
				par--;  
				if (par == 0)
					break;
			}
			i--;
		}
		return i;
	}

	/*
	 * This method searchs the start of a block of code surrounded of parenthesis
	 */

	public static int searchStartBlock(int end, String file){
		int i = end;
		int par = 0;
		while (i > 0) {
			if(file.charAt(i)==')') par++;                              
			else if (file.charAt(i)=='(') par--;
			else if (file.charAt(i)==';' && par == 0) return i;
			i--;
		}		
		return i;
	}

}
