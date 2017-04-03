package com.polimi.thesis;

import java.util.HashMap;
import java.util.Map;

public class Partitioner {

	private String partitioner;
	private static final Map<String, String> partitioners = createMap();
	
	public Partitioner(String method){
		if(partitioners.containsKey(method))
			partitioner = partitioners.get(method);
		else partitioner = "";
	}
	
	public Partitioner(String method, String methodChild){
		if(methodChild.equals("")){
			if(partitioners.containsKey(method))
				partitioner = partitioners.get(method);				
			else partitioner = "";
			System.out.println("Received: " +method+ ", not child method, partition: " +partitioner);
			return;			
		}
		if(partitioners.containsKey(methodChild)){
			partitioner = partitioners.get(methodChild);
			if(partitioners.containsKey(methodChild)){
				String part = partitioners.get(method);
				System.out.println("Received: " +method+ ", childMethod: " +methodChild+ ", partition: " +part);
				if(part != null && !part.equals("preserves")) // If it not preserves we update the value
					partitioner = partitioners.get(method);			
				else partitioner = "";
			} else partitioner = "";
			
		}
		else partitioner = "";
	}


	public String getPartitioner() {
		return partitioner;
	}
	
	public void setPartitioner(String partitioner){
		this.partitioner = partitioner;
	}

	public void setPartitionerOfMethod(String method) {
		if(partitioners.containsKey(method)){
			String part = partitioners.get(method);
			System.out.println("Received: " +method+ ", partition: " +part);
			if(!part.equals("preserves")) // If it not preserves we update the value
				partitioner = partitioners.get(method);			
			System.out.println("Received: " +method+ ", partition set to: " +partitioner);
		}
		else partitioner = "";
	}

	public Boolean checkIfChanges(String method){
		if(partitioners.containsKey(method)){
			String part = partitioners.get(method);
			if(part.equals("preserves"))
				return true;
			return partitioner.equals(partitioners.get(method));			
		}
		else
			return false;
	}

    private static Map<String, String> createMap()
    {
        Map<String,String> myMap = new HashMap<String,String>();
        myMap.put("map", "");
        myMap.put("filter", "preserves");
        myMap.put("flatMap", "");
        myMap.put("mapPartitions", ""); 						// Check
        myMap.put("mapPartitionsWithIndex", ""); 				// Check
        myMap.put("sample", "preserves");
        myMap.put("union", "DONT KNOW HOW TO DO");				// Check
        myMap.put("intersection", "");
        myMap.put("distinct", "");
        myMap.put("groupByKey", "HashPartitioner");
        myMap.put("reduceByKey", "HashPartitioner");
        myMap.put("aggregateByKey", "HashPartitioner");
        myMap.put("sortByKey", "RangePartitioner");
        myMap.put("join", "HashPartitioner");
        myMap.put("cogroup", "preserves");
        myMap.put("cartesian", ""); 							//Check
        myMap.put("pipe", "preserves");
//        myMap.put("coalesce", "CoalescedRDDPartition");		// Check
        myMap.put("coalesce", "DefaultPartitionCoalescer");		// Check
        myMap.put("repartition", "HashPartitioner");
        myMap.put("substract", "");								// Supposed to be in Node but can't find
        myMap.put("parallelize", "ParallelCollectionPartition");
        myMap.put("combineByKey", "HashPartitioner");
        myMap.put("leftOuterJoin", "HashPartitioner");
        myMap.put("rightOuterJoin", "HashPartitioner");
        myMap.put("mapToPair", "");
        myMap.put("textFile", "");								// Check
        
        
        myMap.put("mapValues", "preserves");
        return myMap;
    }

}
