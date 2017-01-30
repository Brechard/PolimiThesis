package com.polimi.thesis;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.polimi.thesis.Variables.MethodsType;

public class FindHelper {

	/*
	 * Method that will find all the variables that represent and RDD
	 */
	public static Set<String> findRDDs(String file){
        System.out.println(">> findRDDs");
    	Set<String> listRDDs = new HashSet<String>();

		String regex = "\\w*=\\w*\\.";

		for (int i = 0; i < Variables.transformations.length; i++) {
		
			// Create a Pattern object
			Pattern r = Pattern.compile(regex+Variables.transformations[i]);
			// Now create matcher object.
			Matcher m = r.matcher(file.replace(" ", ""));
			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
		        listRDDs.add(var);
			}
		}
		for (int i = 0; i < Variables.shuffles.length; i++) {
			
			// Create a Pattern object
			Pattern r = Pattern.compile(regex+Variables.shuffles[i]);
			// Now create matcher object.
			Matcher m = r.matcher(file.replace(" ", ""));
			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
		        listRDDs.add(var);
			}
		}
		
		// Check for RDDs created like newRDD = otherRDD
		regex = "\\w*=";
		List<String> aux = new ArrayList<String>();

		for(String s: listRDDs){
			String search = regex + s;
			// Create a Pattern object
			Pattern r = Pattern.compile(search);
			// Now create matcher object.
			Matcher m = r.matcher(file.replace(" ", ""));
			
			System.out.println(search);			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
				System.out.println("FIND: " +m.group()+", already: " +listRDDs.contains(var));
		        if(!listRDDs.contains(var))
		        	aux.add(var);
			}			
		}
		listRDDs.addAll(aux);
		for (String s : listRDDs)
			System.out.println(">>>>>> " +s);
		return listRDDs;
	}
	
	public static Set<String> findSC(String file){

        System.out.println(">> findSC");
    	Set<String> listSC = new HashSet<String>();

        String regex = "(JavaSparkContext?)\\s*(\\w*?)\\s*=";
		// Create a Pattern object
		
		Pattern r = Pattern.compile(regex);
		// Now create matcher object.
		Matcher m = r.matcher(file);
		
		while(m.find()){
			if (m.group(2) != null && !m.group(2).replaceAll(" ", "").equals(""))
		        listSC.add(m.group(2));
		}
		for (String s : listSC)
			System.out.println(">>>>>> " +s);
		return listSC;
	}	
	
	private static Boolean beforeElse = false;
	private static int posElse;
	private static List<String> ifs = new ArrayList<String>();
	private static List<String> loops = new ArrayList<String>();
	
	public static PairList findIfsAndLoops(String file){

		Pattern r = Pattern.compile("\\w*");
		Matcher m = r.matcher(file);
		while(m.find()){
			if(m.group().replace(" " ,"").equals(""))
				continue;
			String s = m.group();
			int i = m.start();
			if (s.equals("if")){
				if(beforeElse){
					beforeElse = false;
					checkIfContainsSparkMethods(file, i, "else if");	
				} else
					checkIfContainsSparkMethods(file, i, s);	
			} else if (s.equals("else")){
				beforeElse = true;
				posElse = i;
			} else if(beforeElse){
				beforeElse = false;
				checkIfContainsSparkMethods(file, posElse, "else");					
			} 			
			if(s.equals("for") || s.equals("while"))				
				checkIfContainsSparkMethods(file, i, s);	
		}		
		return new PairList(ifs, loops);
	}
	
	private static String condition = ""; // The condition is a global variable in order to save the value for the else, the value is only updated when there are spark methods inside
	public static void checkIfContainsSparkMethods(String file, int start, String ifOrElseOrLoop){

		int endCondition = start;
		if(!ifOrElseOrLoop.equals("else")){ // Search the beginning of the condition
			while(file.charAt(start) != '(' && start < file.length())
				start++;
			endCondition = SearchHelper.searchEndParenthesis(start, file);
		}
		
		int end = endCondition;
		Character c = file.charAt(end);
		while((c != '{' && c != '\n' && !Character.isLetterOrDigit(c) && c != '_') && end < file.length()){
			end++;
			c = file.charAt(end);
		}
		int endIf;
		if(c == '{') // We have to find where it ends
			endIf = SearchHelper.searchEndCurlyBracket(end, file);
		else
			endIf = SearchHelper.searchEndBlock(end, 0, file);			
		
		List<String> methods = findMethods(file.substring(end, endIf), end);
		if(methods.size() != 0){
			if(!ifOrElseOrLoop.equals("else")) condition = file.substring(start + 1, endCondition);
			if(ifOrElseOrLoop.equals("while") || ifOrElseOrLoop.equals("for"))
				loops.add(start+ "-" +ifOrElseOrLoop+ "(" +condition+ ")-" +endIf);
			else
				ifs.add(start+ "-" +ifOrElseOrLoop+ "(" +condition+ ")-" +endIf);	
		}
		System.out.println(start+"-" +endIf+", " +ifOrElseOrLoop+": " +file.substring(start, endIf));
	}
	
	/*
	 * Find the spark methods inside the block sent but not considering those inside parenthesis
	 * The string inside the list will be: positionInBlock - method - positionInFile
	 * @param beginnning Integer that tells the position of the first letter of the block in the file string
	 */
	public static List<String> findMethods(String block, int beginning){
		List<String> methods = new ArrayList<String>();
		Pattern r1 = Pattern.compile("\\w*");
		Matcher m1 = r1.matcher(block);
		int end = 0;
		while (m1.find()) {
			if ((m1.start() - 1) >= 0 && block.charAt(m1.start() - 1) == '.' && CheckHelper.checkMethod(m1.group()) != MethodsType.others) { // Method found
				if (m1.start() > end) {
					methods.add(String.valueOf(m1.start()) +"-"+ m1.group() +"-"+ String.valueOf(m1.start() + beginning));
					end = SearchHelper.searchEndParenthesis(m1.end(), block); // We search only the methods in the main block, not inside the parenthesis
				}
			}
		}
		return methods;
	}

	/*
	 * Finds the block where the rdd passed as parameter is created
	 */
	public static List<Pair> findBlock(String rdd, Integer actualPos, String file){
		
		List<Integer> start = findRDD(rdd, actualPos, file);	
		List<Pair> pairs = new ArrayList<Pair>();
		System.out.println("findBlock(" +rdd+") start: " +start);
		for(Integer i: start){
			pairs.add(new Pair(file.substring(i, SearchHelper.searchEndBlock(i, 0, file)), i));
		}		
		return pairs;
	}
	
	public static String findCache(int end, String block){
		int start = end - 100;
		if (start < 0) start = 0;
		Pattern r = Pattern.compile("\\w*");
		Matcher m = r.matcher(block.substring(start, end));
		String cache = "";
		while(m.find()){
			if (!m.group().replaceAll(" ", "").equals(""))				
				cache = m.group();			
		}
		return cache;			
	}
	
	
	/*
	 * Method that finds where an specific RDD is created
	 */
	public static List<Integer> findRDD(String s, Integer actualPos, String file){
		List<Integer> positions = new ArrayList<Integer>();
        int length = s.length();
        int i = 1;
		// Since the actual position sent is the beginning of the method aplied to it, and we want to avoid to find the same line as we were just send we have to consider that this line could be s = s.method
		int end = actualPos - length*2 - 6;
        System.out.println(">> findRDD: " +s+", actualPos: " +actualPos+ ", file: " +file.length()+ ", end "+end);
        
		while (i < end) {  // Recorremos la expresión carácter a carácter		
			if(file.substring(i, i + length).equals(s)
					&& !Character.isLetterOrDigit(file.charAt(i + length + 1)) && file.charAt(i + length + 1) != '_'
					&& !Character.isLetterOrDigit(file.charAt(i - 1)) && file.charAt(i - 1) != '_') {
				if(CheckHelper.checkIqual(i + length, file)){
					positions.add(i);
				}
			} 
			i++;
		}
		
		return positions;		
	}


}
