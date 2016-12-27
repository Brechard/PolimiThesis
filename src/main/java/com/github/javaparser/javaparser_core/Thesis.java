package com.github.javaparser.javaparser_core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONObject;

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseException;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.javaparser_core.Variables.MethodsType;
import com.google.gson.stream.JsonWriter;

public class Thesis {
	
	public static JsonWriter json;
	public static JSONObject json2 = new JSONObject();
	public static int trans = 0;
	public static int acts = 0;
	public static int stages = 0;

	public static ArrayList<String> methods = new ArrayList<String>();

	public static void main(String[] args) {
		
		
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
		String[] methodsFile = file.split(regex);
		String method = "";
		MethodsType type = null;
		for (int i = 1; i < methodsFile.length; i++) {
			// This methods are applied directly to the JavaSparkContext
			method = methodsFile[i].split("\\(")[0];
			type = CheckMethod(method);
			System.out.println("Method " +i+ ": " +method+ ", type: " +type);
			System.out.println(json);
			
			if (type == MethodsType.action) { 
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
				
				json2.put("Stage: " +acts, new JSONObject().put("Action", method));
				acts++;
				stages++;
				System.out.println("ACTION: " +method);
			}
			else if(type == MethodsType.transformation){
				try{
					json.value(method);					
				} catch (IOException e){
					e.printStackTrace();
				}
				json2.put("Transformation " +trans, method);
				trans++;
				System.out.println("TRANSFORMATION: " +method);
			} 
			System.out.println(json);
			
				
			methods.add(methodsFile[i]);
			String[] insideMethods = methodsFile[i].split("\\.");
			for (int j = 1; j < insideMethods.length; j++) {
				method = insideMethods[j].split("\\(")[0];
				type = CheckMethod(method);
				
				System.out.println(json);
				
				if (type == MethodsType.action) { 
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
					
					json2.put("Stage: " +acts, new JSONObject().put("Action", method));
					acts++;
					stages++;
					System.out.println("ACTION: " +method);
				}
				else if(type == MethodsType.transformation){
					try{
						json.value(method);					
					} catch (IOException e){
						e.printStackTrace();
					}
					json2.put("Transformation " +trans, method);
					trans++;
					System.out.println("TRANSFORMATION: " +method);
				} 
				System.out.println(json);
				
				methods.add(method);
				System.out.println("Method " +i+ ", inside method: " +j+": " +method+ ", type: " +type);
			}
		}

		
		try {
			if (stages > 0)
				json.endArray();				
			json.endObject();
			json.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		

		System.out.println("\n"+json2+"\n\n");
		System.out.println(json);
		
				
		System.out.println("\n\n\ntsparkVariable =" +sparkVariable+ "--------- \n\n");
		System.out.println(file);
	}
	
	// Check if the method receive is an action, a transformation or is some method that is not from spark
	private static MethodsType CheckMethod(String method){
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