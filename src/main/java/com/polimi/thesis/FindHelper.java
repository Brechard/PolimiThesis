package com.polimi.thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.polimi.thesis.Variables.MethodsType;

import scala.tools.asm.util.CheckMethodAdapter;

public class FindHelper {

	/*
	 * Method that will find all the variables that represent and Node
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
			
			while(m.find()){
		        String b =  m.group();
		        String var = b.split("=")[0];
		        if(!listRDDs.contains(var))
		        	aux.add(var);
			}			
		}
		aux.addAll(findRDDsComplex(file));
		listRDDs.addAll(aux);
		for (String s : listRDDs)
			System.out.println(">>>>>> " +s);
		return listRDDs;
	}
	
	private static List<String> findRDDsComplex(String file){
		List<String> aux = new ArrayList<String>();
		String search = "\\w+\\s*=";
		// Create a Pattern object
		Pattern r = Pattern.compile(search);
		// Now create matcher object.
		Matcher m = r.matcher(file);

		while(m.find()){
			String pRDD = m.group().split("=")[0];
			int l = m.start() + m.group().length();
			while(file.charAt(l) == ' ')
				l++;
			if(file.charAt(l) == '('){ // Possible Node created
				int end = SearchHelper.searchEndParenthesis(l, file);
				if(file.charAt(end + 1) == '.'){
					int i = end + 2;
					while(file.charAt(i) == ' ')
						i++;
					int j = i;
					while(Character.isLetterOrDigit(file.charAt(j)) || file.charAt(j) == '_')
						j++;
					String method = file.substring(i,j);
					if(CheckHelper.checkMethod(method) != MethodsType.others)
						aux.add(pRDD.replaceAll(" ", ""));					
				}
					
			}
		}
		return aux;
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
	private static List<Condition> ifs = new ArrayList<Condition>();
	private static List<String> loops = new ArrayList<String>();
	// key = end of condition, value = conditionId - condition;
	private static Map<Integer, String> conditions = new HashMap<Integer, String>(); 
	private static int ifId = 0;
	
	public static PairList findIfsAndLoops(String file, int beggining){
		return findIfsAndLoops(file, beggining, new ArrayList<Integer>(), -1, false, false, "");
	}
	public static PairList findIfsAndLoops(String file, int beggining, List<Integer> idParentCondition, int id, Boolean addLastMethod, Boolean addFirstMethod, String parentType){
		int endIf = beggining;
		Pattern r = Pattern.compile("\\w*");
		Matcher m = r.matcher(file);
//		System.out.println(">>>> Searching inside: "+idParentCondition+", id: " +id);
		while(m.find()){
			if(m.group().replace(" " ,"").equals("") || (m.start() + beggining) < endIf)
				continue;
			String s = m.group();
			int i = m.start();
			if (s.equals("if")){
				if(beforeElse){
					beforeElse = false;
//					System.out.println(">>>>>> A, id: " +id);
					endIf = checkIfContainsSparkMethods(file, i, "else if", beggining, true, idParentCondition, addLastMethod, addFirstMethod, parentType);	
//					System.out.println("<<<<<< A, parents: " +idParentCondition);
				} else {
					ifId++;
//					System.out.println(">>>>>> B, id: " +id);
					endIf = checkIfContainsSparkMethods(file, i, s, beggining, false, idParentCondition, addLastMethod, addFirstMethod, parentType);
//					System.out.println("<<<<<< B, parents: " +idParentCondition);
				}
			} else if (s.equals("else")){
				beforeElse = true;
				posElse = i +s.length();
			} else if(beforeElse){	// There was an else and not and if after it so we have to analyze the else
				beforeElse = false;
//				System.out.println(">>>>>> C, id: " +id);
				endIf = checkIfContainsSparkMethods(file, posElse, "else", beggining, false, idParentCondition, addLastMethod, addFirstMethod, parentType);					
//				System.out.println("<<<<<< C, parents: " +idParentCondition);
			} 			
			if(s.equals("for") || s.equals("while"))
				endIf = checkIfContainsSparkMethods(file, i, s, beggining, false, idParentCondition, addLastMethod, addFirstMethod, parentType);	
		}	
//		System.out.println("<<<< Searching inside: "+idParentCondition+", id: " +id);		
		return new PairList(ifs, loops);
	}
	
	private static String condition = ""; // The condition is a global variable in order to save the value for the else, the value is only updated when there are spark methods inside
	public static int checkIfContainsSparkMethods(String file, int start, String ifOrElseOrLoop, int beggining, Boolean beforeElse, List<Integer> idParentCondition
			, Boolean addLastMethod, Boolean addFirstMethod, String parentType){
		Ints ints = findEndOfCondition(start, ifOrElseOrLoop, file);
		int end = ints.getEnd();
		int endIf = ints.getEndIf();
		int startCondition = ints.getStartCondition();
		int endCondition = ints.getEndCondition();
		start = ints.getStart();
		List<String> methods = findMethods(file.substring(end, endIf), end);
		List<Integer> parents = new ArrayList<Integer>();
		parents.addAll(idParentCondition);
		List<Integer> parentsToSend = new ArrayList<Integer>();
		int id = ifId;
		String lastMethod = "";
		String type = "";
		if(methods.size() != 0){
			if(!ifOrElseOrLoop.equals("else")) condition = file.substring(startCondition + 1, endCondition);
			if(ifOrElseOrLoop.equals("while") || ifOrElseOrLoop.equals("for"))
				loops.add(start+ "-" +ifOrElseOrLoop+ "(" +condition+ ")-" +endIf);
			else {
				int key = start + beggining - 1;
				if(beforeElse)
					key -= 5; // 4 because of "else".length + 1 from the space
				if(conditions.containsKey(key)){
					String[] s = conditions.get(key).split("-", 2);
					condition = s[1];
					id = Integer.valueOf(s[0]);
				}
				type = ifOrElseOrLoop;
				if(beforeElse)
					type = "else if";
				if(parents.contains(id)){
					parents.remove(new Integer(id));
				}
				
				lastMethod = methods.get(methods.size() - 1);
				String lastMethodName = lastMethod.split("-")[1];
				int lastMethodPos = Integer.valueOf(lastMethod.split("-")[2]) + beggining;
				System.out.println("Last method: " +lastMethod+", pos: " +lastMethodPos+", id: " +id);
				
				ifs.add(new Condition(
						id,
						type,
						start + beggining, 
						condition, 
						endIf + beggining, 
						methods.get(0).split("-")[1], 
						Integer.valueOf(methods.get(0).split("-")[2])  + beggining, 
						lastMethodName, 
						lastMethodPos,
						parents));	

				conditions.put(endIf + beggining, id +"-" +condition);

				parentsToSend.addAll(idParentCondition);
				if(!parentsToSend.contains(id))
					parentsToSend.add(id);
				if(addLastMethod){
					for(Condition con: ifs){
						if(idParentCondition.contains(con.getId()) && con.getType().equals(parentType)){
							con.addLastMethod(lastMethodName);
							con.addLastMethodPos(lastMethodPos);
						}
					}
				}
				if(addFirstMethod){
					for(Condition con: ifs){
						if(idParentCondition.contains(con.getId()) && con.getType().equals(parentType)){
							con.addFirstMethod(methods.get(0).split("-")[1]);
							con.addFirstMethodPos(Integer.valueOf(methods.get(0).split("-")[2])  + beggining);
						}
					}
				}
			}
		}
		System.out.println((start + beggining)+"-" +(endIf + beggining)+", " +file.substring(start, endIf));		
		if(methods.size() != 0){
			System.out.println("First method: " +methods.get(0).split("-")[1]+
					", pos: " +(Integer.valueOf(methods.get(0).split("-")[2])  + beggining)+
					", last method: " +methods.get(methods.size() - 1).split("-")[1]+
					", pos: " +(Integer.valueOf(methods.get(methods.size() - 1).split("-")[2]) + beggining));		
		}
		if(methods.size() > 0){
			findIfsAndLoops(file.substring(startCondition, endIf), startCondition + beggining, parentsToSend, id,
					checkMethodIsInside(lastMethod, file.substring(startCondition, endIf), startCondition + beggining),
					checkMethodIsInside(methods.get(0), file.substring(startCondition, endIf), startCondition + beggining), type); 			
		}
		return endIf;
	}
	
	private static Ints findEndOfCondition(int start, String ifOrElseOrLoop, String file){
		int startCondition = start;
		int endCondition = start;
		if(!ifOrElseOrLoop.equals("else")){ // Search the beginning of the condition
			while(file.charAt(startCondition) != '(' && startCondition < file.length())
				startCondition++;
			endCondition = SearchHelper.searchEndParenthesis(startCondition, file);
		} else start -= "else".length();

		int end = endCondition;
		Character c = file.charAt(end);
		while((c != '{' && c != '\n' && !Character.isLetterOrDigit(c) && c != '_') && end < file.length()){
			end++;
			c = file.charAt(end);
		}
		int endIf;
		if(c == '{') // We have to find where it ends
			endIf = SearchHelper.searchEndCurlyBracket(end, file) + 1;
		else {
			endIf = SearchHelper.searchEndBlock(end, 0, file) +1;			
			c = file.charAt(endIf);
			while((!Character.isLetterOrDigit(c) && c != '_') && endIf < file.length()){
				c = file.charAt(endIf);
				endIf++;
			}
			endIf -= 2;
			c = file.charAt(endIf);
		}
		return new Ints(end, endIf, start, startCondition, endCondition);
	}
	
	/*
	 * Check if the last method is inside of a condition, what would mean there is going to be more
	 * than a last method and therefore we will find them in the nested condition	
	 */	
	private static Boolean checkMethodIsInside(String method, String file, int beggining){
		System.out.println("Method: " +method);
		List<String> ifsList = findConditions(file, beggining);
		for(String s: ifsList){
			int start = Integer.valueOf(s.split("-")[0]);
			int end = Integer.valueOf(s.split("-")[1]);
			int startMethod = Integer.valueOf(method.split("-")[0].replace(" ","")) + beggining;
			System.out.println("Start: " +start+", end: " +end+", startMethod: " +startMethod);
			if(start < startMethod && startMethod < end){
				System.out.println("It is inside");
				return true;
			}
		}
		System.out.println("It is not inside");
		return false;
	}

	private static List<String> findConditions(String file, int beggining){
		Pattern r = Pattern.compile("\\w*");
		Matcher m = r.matcher(file);
		List<String> ifsList = new ArrayList<String>(); // startPos - endPos
		// First we have to find if there are some conditions inside the string
		int endIf = 0;
		while(m.find()){
			if(m.group().replace(" " ,"").equals(""))
				continue;
			String s = m.group();
			if (s.equals("if") || s.equals("else")){
				endIf = findEndOfCondition(m.start(), s, file).getEndIf();
				ifsList.add((m.start() + beggining) +"-"+ (endIf + beggining));
			} 			
		}
		return ifsList;		
	}
	
	/*
	 * Find the spark methods inside the block sent but not considering those inside parenthesis
	 * The string inside the list will be: positionInBlock - method - positionInFile
	 * @param beginnning Integer that tells the position of the first letter of the block in the file string
	 */
	public static List<String> findMethods(String block, int beginning){
		List<String> methods = new ArrayList<String>();
		Pattern r1 = Pattern.compile("\\w+");
		Matcher m1 = r1.matcher(block);
		int end = 0;
		while (m1.find()) {
			if((m1.start() - 1) >= 0 && block.charAt(m1.start() - 1) == '(')
				end = SearchHelper.searchEndParenthesis(m1.start() - 1, block); // We search only the methods in the main block, not inside the parenthesis
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
		for(Integer i: start){
			int a = file.substring(i).indexOf('.') + i + 1;
			System.out.println(">>> findBlock, rdd: " +rdd+" -> Start: " +i+", continuing: " +
					file.substring(i, i + 25)+", index: " +a+", cont: " +file.substring(a, a+7));
			pairs.add(new Pair(file.substring(i, SearchHelper.searchEndBlock(i, 0, file)), i, a));
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
	 * Method that finds where an specific Node is created
	 */
	public static List<Integer> findRDD(String s, Integer actualPos, String file){
		List<Integer> positions = new ArrayList<Integer>();
        int length = s.length();
        int i = 1;
		// Since the actual position sent is the beginning of the method aplied to it, and we want to avoid to find the same line as we were just send we have to consider that this line could be s = s.method
		int end = actualPos - length*2 - 6;        
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

	private static class Ints {
		private int end;
		private int endIf;
		private int start;
		private int startCondition;
		private int endCondition;

		public Ints(int end, int endIf, int start, int startCondition, int endCondition) {
			super();
			this.setEnd(end);
			this.setEndIf(endIf);
			this.setStart(start);
			this.setStartCondition(startCondition);
			this.setEndCondition(endCondition);
		}

		public int getEnd() {
			return end;
		}

		public void setEnd(int end) {
			this.end = end;
		}

		public int getStartCondition() {
			return startCondition;
		}

		public void setStartCondition(int startCondition) {
			this.startCondition = startCondition;
		}

		public int getEndIf() {
			return endIf;
		}

		public void setEndIf(int endIf) {
			this.endIf = endIf;
		}

		public int getEndCondition() {
			return endCondition;
		}

		public void setEndCondition(int endCondition) {
			this.endCondition = endCondition;
		}

		public int getStart() {
			return start;
		}

		public void setStart(int start) {
			this.start = start;
		}
	}
}
