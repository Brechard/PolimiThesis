package com.github.javaparser.javaparser_core;

public class Variables 
{
	public enum MethodsType{
		action, transformation, others
	}
	
	public static String path = "/home/rodrigo/workspace/cspark/src/main/java/cspark/Chapter4.java";	

	public static String[] actions = 
		{"reduce", "collect", "count", "first", "take", "takeSample", "takeOrdered","saveAsTextFile","saveAsSequenceFile", "saveAsObjectFile","countByKey","foreach"
				
		
				,"collectAsMap", "textFile"};
	
	public static String[] transformations = 
		{"map", "filter", "flatMap", "mapPartitions", "mapPartitionsWithIndex", "sample", "union", "intersection", "distinct", "groupByKey", "reduceByKey",
				"aggregateByKey", "sortByKey", "join", "cogroup", "cartesian", "pipe", "coalesce", "repartition", "repartitionAndSortWithinPartitions"
				
				, "mapToPair"};
	
	
	public static String jsonString = "";
	public static String JSONPath = "methods.json";
	    
}
