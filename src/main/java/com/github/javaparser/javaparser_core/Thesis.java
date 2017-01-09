package com.github.javaparser.javaparser_core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.tree.DefaultMutableTreeNode;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.javaparser_core.Variables.MethodsType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class Thesis {
	
	private static int stages;
	private static int jobs;
	private static String file;
	
	private static Set<String> listRDDs = new HashSet<String>();
	private static Set<String> listSC = new HashSet<String>();
	
	private static TreeMap<Integer, String> map = new TreeMap<Integer, String>(Collections.reverseOrder());
	private static List<String> actionList = new ArrayList<String>();
	
	private static DefaultMutableTreeNode root;
	
	public static void main(String[] args) throws Exception {
		jobs = 0; stages = 0; 

		// creates an input stream for the file to be parsed
        FileInputStream in = null;
		try {
			in = new FileInputStream(Variables.path);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

        // parse the file
        CompilationUnit cu = null;
		try {
			cu = JavaParser.parse(in);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		file = cu.toStringWithoutComments();
		file = file.split("public static void main")[1];
		
		root = new DefaultMutableTreeNode("Jobs");
		
		findRDDs();
		findSC();		
		
		
		
		
		// Create a Pattern object
		Pattern r = Pattern.compile("\\w*");
		// Now create matcher object.
		Matcher m = r.matcher(file);
		
		String cache = "";
		while (m.find()) {
			if ((m.start() - 1) >= 0 && file.charAt(m.start() - 1) == '.') {
				// Now we know that m.group() is a method, we have to check if it is a spark method
				MethodsType type = checkMethod(m.group());
				if (type == MethodsType.action) {
					String s = m.group();
					jobs++;
					if (file.charAt(m.start() - 2) != ')' && (checkRDD(cache) || checkSC(cache))) 
						s = cache+ "." +m.group();
//					map.put(m.start() - 1, s); We are not using TreeMap because it is too slow
					actionList.add(String.valueOf(m.start() +"-"+s));
				} else if (type == MethodsType.shuffle)
					stages++;
			}
			if(!m.group().replaceAll(" ", "").equals(""))
				cache = m.group();
		}

		for(int i = 0; i < actionList.size(); i++) { // We iterate over the map with the spark methods
			addChild(String.valueOf(i)+ "-" +actionList.get(i).split("-")[1], root);
		}
		for(int i = actionList.size() - 1; i >= 0; i--) { // We iterate over the map with the spark methods
			jobs--;
			int key = Integer.valueOf(actionList.get(i).split("-")[0]);
			String method = actionList.get(i).split("-")[1];
			String rddOrSc = "";
			if (method.contains(".")) {				
				String[] w = method.split("\\.");
				method = w[w.length - 1];				
				rddOrSc = w[0];
			}
  
			char c = file.charAt(key - 2);
			// We have to find the block of code that this action refers to
			if(c != ')'){
				if (checkRDD(rddOrSc)) { // If it is applied to an RDD we have to find where and how it was created
					generate(findBlock(rddOrSc), (DefaultMutableTreeNode) root.getChildAt(jobs));
				} else { // If it is applied to an sc there is an error
					throw new Exception("An action cannnot be applied to an SC");
				}
			} else {
				generate(file.substring(searchStartBlock(key), key), (DefaultMutableTreeNode) root.getChildAt(jobs));
			}
			System.out.println("Action found: " +method+ ", a new job should be generated, jobs = "+jobs);
		}
		
		printTree();
				
	}
	
	public static boolean checkCombine(String method){
		for (int i = 0; i < Variables.combineMethods.length; i++) {
			if(method.equals(Variables.combineMethods[i]))
				return true;		
		}
		return false;
	}
	
	public static boolean checkRDD(String word){
//		System.out.println("checkRDD: " +word);
		for(String s: listRDDs){
			if (s.equals(word)) 
				return true;
		}
		return false;
	}
	
	public static boolean checkSC(String word){
//		System.out.println("checkSC: " +word);
		for(String s: listSC){
			if (s.equals(word)) 
				return true;
		}
		return false;
	}
	
	// Check if the method receive is a shuflle method, an action, a transformation or is some method that is not from spark
	public static MethodsType checkMethod(String method){
		
		// It is important to check first if it is a shuflle method because TEXTFILE is considered a shuffle and action in the variables and here we want it to be a shuffle
		if(method.matches(".*By.*") // Check if the method passed will shuffle, considering that any transformation of the kind *By or *ByKey can result in shuffle
				&& !method.equals("groupByKey")) // GroupByKey does not shuffle
			return MethodsType.shuffle;
		else {
			for (int i = 0; i < Variables.shuffles.length; i++) {
				if(method.equals(Variables.shuffles[i]))
					return MethodsType.shuffle;		
			}
		}
		
		for (int i = 0; i < Variables.actions.length; i++) {
			if(method.equals(Variables.actions[i]))
				return MethodsType.action;
		}
		for (int i = 0; i < Variables.transformations.length; i++) {
			if(method.equals(Variables.transformations[i]))
				return MethodsType.transformation;
		}
		return MethodsType.others;
	}
	

	/*
	 * Finds the block where the rdd passed as parameter is created
	 */
	public static String findBlock(String rdd){
		int start = findRDD(rdd);		
		return file.substring(start, searchEndBlock(start, 1, file));
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
	public static int findRDD(String s){
		
//        System.out.println(">> findRDD: " +s);
        int length = s.length();
        int i = 1;
        
		while (i < (file.length() - length)) {  // Recorremos la expresión carácter a carácter
			
			if(file.substring(i, i + length).equals(s)
					&& !Character.isLetterOrDigit(file.charAt(i + length + 1)) && file.charAt(i + length + 1) != '_'
					&& !Character.isLetterOrDigit(file.charAt(i - 1)) && file.charAt(i - 1) != '_') {
				break;
			} 
			i++;
		}
		
		return i;
		
	}

	
	/*
	 * Method that will find all the variables that represent and RDD
	 */
	public static void findRDDs(){
        System.out.println(">> findRDDs");

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

		for (String s : listRDDs)
			System.out.println(">>>>>> " +s);
	}
	
	public static void findSC(){

        System.out.println(">> findSC");

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
		
	}	
		
	
	public static void printTree(){
	
		String result = "\n";
	    Enumeration<?> enumer = root.preorderEnumeration();
	    while (enumer.hasMoreElements()) {
	        DefaultMutableTreeNode node = (DefaultMutableTreeNode) enumer.nextElement();
	        String nodeValue = String.valueOf(node.getUserObject());
	        String indent = "";
	        while (node.getParent() != null) {
	           indent += "    "; 
	           node = (DefaultMutableTreeNode) node.getParent();
	        }
	        result += indent + nodeValue + "\n";
	    }
        System.out.println(result);
        
	}
	
	private static DefaultMutableTreeNode addChild(String title, DefaultMutableTreeNode parent) {
		DefaultMutableTreeNode child = new DefaultMutableTreeNode(title);
		parent.add(child);
		return child;
	}
	
	
	public static int searchEndHelper(int start, String block){
		int i = start;
		int par = 0;
		while (i < block.length()) {
			if(block.charAt(i)=='(')
				par++;
			else if  (block.charAt(i)==')'){
				par--;
				if (par == 0)
					return i;
			}
			i++;
		}
		return i;
	}
	
	/*
	 * This method searchs the end of a block of code given the begining 
	 */
	public static int searchEndBlock(int start, int par2, String block){
//		System.out.println("start at: " +start);
		int i = searchEndHelper(start, block);
		int par = par2;
		if (i == start) {
			return i;
		}
		while (i < block.length()) {
			if(block.charAt(i) == '('){
				i = searchEndBlock(i, par+1, block); // If we find another opening parenthesis we have to reach the end of it again
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
	public static int searchStartBlock(int end){
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

	
	/*
	 * Analyzes a block of code in order to find the relations inside the block
	 */
	public static void generate(String block, DefaultMutableTreeNode parent){
		
//		System.out.println("\n>> generate");
//		System.out.println("Block: " +block+"\n");
		
		// Create a Pattern object (words)
		Pattern r = Pattern.compile("\\w*");
		// Now create matcher object.
		Matcher m = r.matcher(block);
		
		String cache = "";
		int i = 0;
		String rdd = "";
		List<String> forward = new ArrayList<String>(); // The string will be: position - method, that way we don't need to use a TreeMap that is very slow		
		List<DefaultMutableTreeNode> parents = new ArrayList<DefaultMutableTreeNode>();
		// First we read the block to create the relations inside this block backwards, in order to do it we need to read it forward and analyze it backwards		
		// Now create matcher object.
		Matcher m1 = r.matcher(block);
		int end = 0;
		while (m1.find()) {
			if ((m1.start() - 1) >= 0 && block.charAt(m1.start() - 1) == '.' && checkMethod(m1.group()) != MethodsType.others) { // Method found
				if (m1.start() > end) {
					forward.add(String.valueOf(m1.start()) +"-"+ m1.group());
					end = searchEndHelper(m1.end(), block);
				}
			}
		}
		int p = 0;
		// Create dependencies of the block passed
		for (int j = forward.size() - 1; j >= 0; j--) {
			String method = forward.get(j).split("-")[1];
			
			if (checkMethod(method) == MethodsType.shuffle)
				parent = addChild(method, parent);
			else addChild(method, parent);
			parents.add(p++, parent);
//			System.out.println("Method: " +method);
		}
		p = 0;
		// Create dependencies with blocks that appear as variables or inside parenthesis
		if (forward.size() > 0){
			for (int j = forward.size(); j >= 0; j--) {
				
				if (j == forward.size() && forward.size() > 0) {
					int pos = Integer.valueOf(forward.get(j - 1).split("-")[0]);
					String method = forward.get(j - 1).split("-")[1];
					int start = pos + method.length();
//					System.out.println("Second loop, check if last method is combine, start: " +block.charAt(start)+ ", +1: " +block.charAt(start + 1));
					if(block.charAt(start + 1) != ')'){ // The last method of the block is combine Method
						int endBlock = searchEndBlock(start, 1, block);
						cache = findCache(endBlock, block);
						if (checkRDD(cache) || checkSC(cache)) { 
//							System.out.println("Combine method with variable: " +cache);
							generate(findBlock(cache), parents.get(p));					
						} else {
							String subBlock = block.substring(start, end +1);
//							System.out.println("Written combine method: " +subBlock);
							generate(subBlock, parents.get(p));
						}					
					} 
//					else 					
//						System.out.println("No it is not");

					continue;
				}
				
				int pos = Integer.valueOf(forward.get(j).split("-")[0]);
//				System.out.println("Second loop, method: " +forward.get(j).split("-")[1]);

				if(block.charAt(pos - 2) == ')'){ // Method not applied directly to a variable, we have to analyze the interior of the parenthesis
					cache = findCache(pos, block);
//					System.out.println("Cache: " +cache);
					if (block.charAt(pos - 3) == '(') { // There is nothing inside the parenthesis, the method is applied to the result of another method
//						System.out.println("Nothing to do here");
					} else { // Inside of the parenthesis there is something what means that the method before (writing) is a combine method 
						if (checkRDD(cache) || checkSC(cache)) { 
//							System.out.println("Combine method with variable: " +cache);
							generate(findBlock(cache), parents.get(p));					
						} else {
							String subBlock = block.substring(searchStartParenthesisBlock(block, pos), pos);
//							System.out.println("Written combine method: " +subBlock);
							generate(subBlock, parents.get(p));
						}					
					}					
				} else {
					cache = findCache(pos, block);
//					System.out.println("Applied directly to a variable: " +cache);
					if (checkRDD(cache) || checkSC(cache)) {
//						System.out.println("Let's recall");
						generate(findBlock(cache), parents.get(p));					
					} else {
						System.err.println("Error in the code?");
					}
				}
				p++;
			}
		}
	}
}