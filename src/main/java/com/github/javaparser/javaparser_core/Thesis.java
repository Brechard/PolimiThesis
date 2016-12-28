package com.github.javaparser.javaparser_core;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.javaparser_core.Variables.MethodsType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonWriter;

public class Thesis {
	
	private static JsonWriter json;
//	private static JSONObject json2 = new JSONObject();
	private static int trans;
	private static int acts;
	private static int stages;
	private static String[] methodsFile;
	private static int jobs;
	
	private static ArrayList<String> methods = new ArrayList<String>();
	
	private static Map<String, Map<String, ArrayList<String>>> jobsMap = new HashMap<String, Map<String, ArrayList<String>>>();

	public static void main(String[] args) {
		jobs = 0; stages = 1; acts = 0; trans = 0;
			
		try {
			json = new JsonWriter(new FileWriter(Variables.JSONPath));
			json.beginObject();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
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
		
		String file = cu.toStringWithoutComments();
		
		
		
		// Remove the initial code until the actual code inside the main
		String[] fileArray  = file.split("\\)\\s*\\{");
		file = fileArray[1];
		for (int i = 2; i < fileArray.length; i++) {
			file += "){" +fileArray[i];
		}
		
		// Remove the last 2 brackets remaining that end the main method and the java class
		fileArray = file.split("\\}");
		file = "";
		for (int i = 0; i < fileArray.length -2; i++) {
			file += fileArray[i];
		}
				
		
		// Get the name of the variable JavaSparkContext
		String sparkVariable = file.split("JavaSparkContext")[1].replace(" ","").split("=")[0];
		
		// Get the methods used in all the file starting once the JavaSparkContext is created and add them to the list
		String regex = sparkVariable + "\\." ;
		methodsFile = file.split(regex);
		String method = "";
		for (int i = 1; i < methodsFile.length; i++) {
			// This methods are applied directly to the JavaSparkContext
			method = methodsFile[i].split("\\(")[0];
			
			fillMap(method);
			
			
			
//			fillJSON(method);
			String[] insideMethods = methodsFile[i].split("\\.");

			for (int j = 1; j < insideMethods.length; j++) {
				method = insideMethods[j].split("\\(")[0];
				fillMap(method);
	//			fillJSON(method);
			}
		}

		/*
		try {
			if (stages > 0)
				json.endArray();				
			json.endObject();
			json.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/

//		System.out.println("\n\n"+json2+"\n\n");		
		System.out.println("\n\nNumber of actions: " +acts+ " Number of transformations: " +trans+ "\n\n");		
		for(String s: methods){
			System.out.println("Method: " +s);
		}
				
		System.out.println("\n\nsparkVariable =" +sparkVariable+ "--------- \n\n");
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		String jsonString = gson.toJson(jobsMap);
		System.out.println(jsonString);
		
	}
	
	private static void fillMap(String method){
		
		MethodsType type = CheckMethod(method);
		
//		System.out.println("Method : " +method+ ", type: " +type);
		
		// If the method sent is an action a new job should be created
		if (type == MethodsType.action){
			
			jobs++;
			Map<String, ArrayList<String>> stageMap = new HashMap<String, ArrayList<String>>();
			ArrayList<String> methodsList = new ArrayList<String>();
			methodsList.add(method);
			stageMap.put("1", methodsList);
			jobsMap.put(String.valueOf(jobs), stageMap);
			
			System.out.println("ACTION: " +method);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String jsonString = gson.toJson(jobsMap);
			System.out.println(jsonString);

			acts++;
		}
		
		// If the method sent will shuffle it means we have to create a new stage
		
		else if (type == MethodsType.shuffle) { 
				
			stages++;
			
			Map<String, ArrayList<String>> stageMap;
			if (jobsMap.containsKey(String.valueOf(jobs)))
				stageMap = jobsMap.get(String.valueOf(jobs));				
			else 
				stageMap = new HashMap<String, ArrayList<String>>();
			
			ArrayList<String> methodsList = new ArrayList<String>();
			methodsList.add(method);
			stageMap.put(String.valueOf(stages), methodsList);
			jobsMap.put(String.valueOf(jobs), stageMap);
						
			System.out.println("SHUFFLE: " +method+ ", stages: "+stages);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String jsonString = gson.toJson(jobsMap);
			System.out.println(jsonString);

			methods.add(method);
		} 
		// If the method sent is a transformation we keep in the same stage
		
		else if(type == MethodsType.transformation){
			
			Map<String, ArrayList<String>> stageMap = jobsMap.get(String.valueOf(jobs));			
			ArrayList<String> methodsList = stageMap.get(String.valueOf(stageMap.size()));
			methodsList.add(method);
			stageMap.put(String.valueOf(stageMap.size()), methodsList);
			jobsMap.put(String.valueOf(jobs), stageMap);

			trans++;
			System.out.println("TRANSFORMATION: " +method);
			Gson gson = new GsonBuilder().setPrettyPrinting().create();
			String jsonString = gson.toJson(jobsMap);
			System.out.println(jsonString);

			methods.add(method);
		} 		
		
	}
	
	private static void fillJSON(String method){
		MethodsType type = CheckMethod(method);
		System.out.println("Method : " +method+ ", type: " +type);
		
		if (type == MethodsType.shuffle || methods.size() == 0) { 
			try{
				if (stages > 0) {
					json.endArray();
				}
				json.name("Stage: "+ stages);
				json.beginArray();
				json.value(method);					
			} catch (IOException e){
				e.printStackTrace();
			}
			
//			json2.put("Stage: " +acts, new JSONObject().put("Action", method));
			stages++;
			System.out.println("SHUFFLE: " +method);
			methods.add(method);
		} else if(type == MethodsType.transformation || type == MethodsType.action){
			try{
				json.value(method);					
			} catch (IOException e){
				e.printStackTrace();
			}
//			json2.put("Transformation " +trans, method);
			if(type == MethodsType.transformation){
				trans++;
				System.out.println("TRANSFORMATION: " +method);								
			} else {
				acts++;
				System.out.println("ACTION: " +method);								
			}
			methods.add(method);
		} 					
	}
	
	
	
	// Check if the method receive is an action, a transformation or is some method that is not from spark
	private static MethodsType CheckMethod(String method){

		if(method.matches(".*By.*")) // Check if the method passed will shuffle, considering that any transformation of the kind *By or *ByKey can result in shuffle
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
}