package com.polimi.thesis;

public class Variables 
{
	public enum MethodsType{
		action, transformation, shuffle, others
	}
	
//	public static String path = "/home/rodrigo/workspace/cspark/src/main/java/cspark/SparkJoins.java";	
	public static String path = "/home/rodrigo/workspace/cspark/src/main/java/cspark/BasicConditionalTest.java";	

	public static String pathJSON = "/home/rodrigo/workspace/output.json";	

	public static String[] actions = 
		{ "reduce", "collect", "count", "first", "take", "takeSample", "takeOrdered","saveAsTextFile","saveAsSequenceFile", "saveAsObjectFile","countByKey","foreach"
				
		
				,"collectAsMap","aggregate"};
	
	public static String[] transformations = 
		{ "map", "filter", "flatMap", "mapPartitions", "mapPartitionsWithIndex", "sample", "union", "intersection", "distinct", "groupByKey", "reduceByKey",
				"aggregateByKey", "sortByKey", "join", "cogroup", "cartesian", "pipe", "coalesce", "repartition", "repartitionAndSortWithinPartitions"
				, "substract"

				
				,"parallelize","combineByKey", "mapToPair","textFile"};
	
	// A method will shuffle is the partitioner has not been cleared
	public static String[] shuffles = {"cogroup", "distinct", "leftOuterJoin", "rightOuterJoin","sortByKey", "join", "groupByKey"};
		
	public static String jsonString = "";
	public static String JSONPath = "methods.json";
	
	public static String[] combineMethods = { "join", "union", "intersection", "substract", "cartesian","leftOuterJoin","rightOuterJoin", "reduceByKey"};
	
	public static String[] withInputMethods = { "textFile", "mapToPair", "leftOuterJoin", "rightOuterJoin" };

	// Any method of the kind *By or *ByKey can result in shuffle, that's why here we will place the By method's that we know are transformation and will not create a new stage
	public static String[] methodsByTransformation = {"combineByKey", "groupByKey" };

	public static String[] clearsPartitioner = { "cartesian", "map", "mapToPair", "flatMap", "union", "intersection", "distinct", "sortByKey", "repartition"};
	public static String[] preservesPartitioner = { "filter", "sample", "join", "cogroup",  "pipe", "repartitionAndSortWithinPartitions", "parallelize", "textFile", "leftOuterJoin", "rightOuterJoin"};

	public static String[] dependsOnPartitioner = {"join" , "cogroup", "leftOuterJoin", "rightOuterJoin", "reduceByKey", "combineByKey", "groupByKey", "aggregateByKey"};
	// coalesce -> if shuffle clearrPartitioner
	// substract not found (supposed to be in rdd)
	
	/*
	 * Methods not sure about:
	 * groupByKey, reduceByKey, aggregateByKey, combineByKey, cartesian
	 */
	
	/*
	 * mapPartitions, mapPartitionsWithIndex,
	 */
	   // `preservesPartitioning` indicates whether the input function preserves the partitioner, which
	   // should be `false` unless this is a pair RDD and the input function doesn't modify the keys.

}

