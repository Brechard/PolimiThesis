package com.github.javaparser.javaparser_core;

import java.io.IOException;

import com.github.javaparser.javaparser_core.Variables.MethodsType;

public class Variables 
{
	public enum MethodsType{
		action, transformation, shuffle, others
	}
	
	public static String path = "/home/rodrigo/workspace/cspark/src/main/java/cspark/SparkJoins.java";	

	public static String[] actions = 
		{ "reduce", "collect", "count", "first", "take", "takeSample", "takeOrdered","saveAsTextFile","saveAsSequenceFile", "saveAsObjectFile","countByKey","foreach"
				
		
				,"collectAsMap"};
	
	public static String[] transformations = 
		{ "map", "filter", "flatMap", "mapPartitions", "mapPartitionsWithIndex", "sample", "union", "intersection", "distinct", "groupByKey", "reduceByKey",
				"aggregateByKey", "sortByKey", "join", "cogroup", "cartesian", "pipe", "coalesce", "repartition", "repartitionAndSortWithinPartitions"
				, "substract", "cartesian"

				
				, "mapToPair","textFile"};
	
	// TextFile may not be exactly a shuffle but it creates a new stage that is why we will consider it as a shuffle method and also transformation 
	public static String[] shuffles = { "repartition", "join", "cogroup", "distinct", "leftOuterJoin", "rightOuterJoin","sortByKey", "textFile"};
		
	public static String jsonString = "";
	public static String JSONPath = "methods.json";
	
	public static String[] combineMethods = { "join", "union", "intersection", "substract", "cartesian","leftOuterJoin","rightOuterJoin"};

}

