package com.github.javaparser.javaparser_core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.javaparser_core.Variables.MethodsType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

public class Thesis {
	
	private static int trans;
	private static int acts;
	private static int stages;
	private static String[] methodsFile;
	private static int jobs;
	private static String file;
	private static ArrayList<String> methods = new ArrayList<String>();
	
	private static Map<String, Map<Integer, ArrayList<String>>> jobsMap = new HashMap<String, Map<Integer, ArrayList<String>>>();
	private static Set<String> listRDDs = new HashSet<String>();

	private static boolean newJob;
	
	public static void main(String[] args) {
		jobs = 0; stages = 0; acts = 0; trans = 0; newJob = true;
		
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
//		System.out.println(file);
		
		// Remove the initial code until the actual code inside the main
		/*
		String[] fileArray  = file.split("\\).*\\s*\\{");
		file = fileArray[1];
		System.out.println(">>>>>>>>>>>>>>>>>>> " +file);
		for (int i = 2; i < fileArray.length; i++) {
			file += "){" +fileArray[i];
		}
		*/
		
		// Remove the last 2 brackets remaining that end the main method and the java class
		/*
		fileArray = file.split("\\}");
		file = "";
		for (int i = 0; i < fileArray.length -2; i++) {
			file += fileArray[i];
		}
		*/
						
		// Get the name of the variable JavaSparkContext		
		String sparkVariable = file.split("JavaSparkContext")[1].replace(" ","").split("=")[0];
		
		findRDDs();
		
		// Get the methods used in all the file starting once the JavaSparkContext is created and add them to the list
		String regex = sparkVariable + "\\." ;
		methodsFile = file.split(regex);
		String method = "";
		for (int i = 1; i < methodsFile.length; i++) {
			
			// This methods are applied directly to the JavaSparkContext
			method = methodsFile[i].split("\\(")[0];
			
			
			
			
			// Create a Pattern object
			Pattern r = Pattern.compile("\\w*");
			// Now create matcher object.
			Matcher m = r.matcher(methodsFile[i - 1]);

			String s = "";
			while (m.find()) {
				if (listRDDs.contains(m.group())) {
					s = m.group();
				}
			}
			if (!s.equals("")) {
				System.out.println(">> RDD: " +s+ ", i = " +i);																				
			}
			
			
			
			
			
			
			
			String[] insideMethods = methodsFile[i].split("\\.");			
			fillMap(method);
			
			
			
//			fillJSON(method);

			for (int j = 1; j < insideMethods.length; j++) {
				method = insideMethods[j].split("\\(")[0];

				
				// Create a Pattern object
				r = Pattern.compile("\\w*");
				// Now create matcher object.
				m = r.matcher(insideMethods[j - 1]);

				while (m.find()) {
					if (listRDDs.contains(m.group())) {
						System.out.println(">> RDD: " +m.group()+ ", j = " +j);																				
					}
				}

				
				
				
				
				
				
				
				
				
				
				
				fillMap(method);
	//			fillJSON(method);
			}
		}

		System.out.println("\n\nNumber of actions: " +acts+ ". Number of transformations: " +trans+ ". Number of stages: " +stages+"\n\n");		
		for(String s: methods){
			System.out.println("Method: " +s);
		}
				
		System.out.println("\n\nsparkVariable =" +sparkVariable+ "--------- \n\n");
		
//		prettyPrint();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(jobsMap);
		System.out.println(jsonString);
	
	}
	
	
	public static boolean checkCombine(String method){
		for (int i = 0; i < Variables.combineMethods.length; i++) {
			if(method.equals(Variables.combineMethods[i]))
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

		for (String s : listRDDs)
			System.out.println(">>>>>>" +s);
	}
	
	public static void fillMap(String method){
		
		MethodsType type = checkMethod(method);
		
//		System.out.println("Method : " +method+ ", type: " +type);
		
		
		if (newJob && (type != MethodsType.others)){
			System.out.println(">>>>>>>>> newJob");
			newJob = false;
			jobs++;
			stages++;

			Map<Integer, ArrayList<String>> stageMap = new HashMap<Integer, ArrayList<String>>();
			ArrayList<String> methodsList = new ArrayList<String>();
			methodsList.add(method);
			stageMap.put(stages, methodsList);
			jobsMap.put(String.valueOf(jobs), stageMap);
			
			prettyPrint();
			acts++;
		}
		
		// If the method sent will shuffle it means we have to create a new stage
		
		else if (type == MethodsType.shuffle) { 				
			stages++;
			System.out.println("SHUFFLE: " +method+ ", stages: "+stages);
			
			Map<Integer, ArrayList<String>> stageMap;
			if (jobsMap.containsKey(String.valueOf(jobs)))
				stageMap = jobsMap.get(String.valueOf(jobs));				
			else 
				stageMap = new HashMap<Integer, ArrayList<String>>();
			
			ArrayList<String> methodsList = new ArrayList<String>();
			methodsList.add(method);
			stageMap.put(stages, methodsList);
			jobsMap.put(String.valueOf(jobs), stageMap);
						
			prettyPrint();
			methods.add(method);
		} 
		// If the method sent is a transformation we keep in the same stage
		
		else if(type == MethodsType.transformation){
			System.out.println("TRANSFORMATION: " +method+ ", stages: " +stages+ ", jobs:" +jobs);
			
			Map<Integer, ArrayList<String>> stageMap = jobsMap.get(String.valueOf(jobs));
			ArrayList<String> methodsList = stageMap.get(stages);
			methodsList.add(method);
			stageMap.put(stages, methodsList);
			jobsMap.put(String.valueOf(jobs), stageMap);

			trans++;
			prettyPrint();
			methods.add(method);
		} 	
		// If the method sent is an action a new job should be created (a Job "ends" with an action)
		if (type == MethodsType.action) {
			newJob = true;
			System.out.println("ACTION: " +method+ ", stages: "+stages);
		}

	}
	
	public static void prettyPrint(){
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(jobsMap);
		System.out.println(jsonString);
	}
}