package com.github.javaparser.javaparser_core;

import java.io.IOException;

import com.github.javaparser.javaparser_core.Variables.MethodsType;

public class Variables 
{
	public enum MethodsType{
		action, transformation, shuffle, others
	}
	
	public static String path = "/home/rodrigo/workspace/cspark/src/main/java/cspark/Chapter4.java";	

	public static String[] actions = 
		{ "reduce", "collect", "count", "first", "take", "takeSample", "takeOrdered","saveAsTextFile","saveAsSequenceFile", "saveAsObjectFile","countByKey","foreach"
				
		
				,"collectAsMap", "textFile"};
	
	public static String[] transformations = 
		{ "map", "filter", "flatMap", "mapPartitions", "mapPartitionsWithIndex", "sample", "union", "intersection", "distinct", "groupByKey", "reduceByKey",
				"aggregateByKey", "sortByKey", "join", "cogroup", "cartesian", "pipe", "coalesce", "repartition", "repartitionAndSortWithinPartitions"
				
				, "mapToPair"};
	public static String[] shuffles = { "repartition", "join", "cogroup"};
		
	public static String jsonString = "";
	public static String JSONPath = "methods.json";
	   
/*
private static void fillJSON(String method){
	MethodsType type = CheckMethod(method);
	System.out.println("Method : " +method+ ", type: " +type);
	if (type == MethodsType.shuffle) { 
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
		
//		json2.put("Stage: " +acts, new JSONObject().put("Action", method));
		stages++;
		System.out.println("SHUFFLE: " +method);
		methods.add(method);
	}
	else if(type == MethodsType.transformation || type == MethodsType.action){
		try{
			json.value(method);					
		} catch (IOException e){
			e.printStackTrace();
		}
//		json2.put("Transformation " +trans, method);
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
*/
}

