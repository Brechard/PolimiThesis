package com.polimi.thesis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateStages {
	
	public static Map<Integer, Stage> create(Map<Integer, Stage> stagesList){
		Stage stage;
		for (Map.Entry<Integer, Stage> entry : stagesList.entrySet()){
			stage = entry.getValue();
			List<RDD> listRDDs = stage.getRDDs();
			
		}

		
		
		
		return stagesList;
	}

}
