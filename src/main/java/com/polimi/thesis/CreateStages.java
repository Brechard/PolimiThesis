package com.polimi.thesis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.polimi.thesis.Variables.MethodsType;

public class CreateStages {
	
	private static Stage stage;
	private static Map<Integer, Stage> stagesMap;
	
	public static PairMapInt create(Map<Integer, Stage> stagesMap2, int stages){
		System.out.println("\n\n\nCreateStages");
		stagesMap = new HashMap<Integer, Stage>();
		Partitioner partitioner = new Partitioner("");
		for (Map.Entry<Integer, Stage> entry : stagesMap2.entrySet()){
			stage = entry.getValue();
			List<RDD> listRDDs = stage.getRDDs();
			Map<Integer, Integer> mapStages = new HashMap<Integer, Integer>(); // key = rddId, value = stageId where the rdd is
			Map<Integer, RDD> mapRDDs = new HashMap<Integer, RDD>(); 
			for(int i = 0; i < listRDDs.size(); i++){
				mapRDDs.put(listRDDs.get(i).getId(), listRDDs.get(i));
			}
			for(int i = 0; i < listRDDs.size(); i++){
				RDD rdd = listRDDs.get(i);
				String method = rdd.getCallSite().split(" at ")[0];
				MethodsType type = CheckHelper.checkMethod(method);
				List<Integer> childrenIds = rdd.getChildrenId();
				// Right now it is only possible that the rdd has one child or cero, because we haven't merged yet the stages that are equal
				if(childrenIds.size() > 0){
					RDD childRDD = mapRDDs.get(childrenIds.get(0));
					String childMethod = childRDD.getCallSite().split(" at ")[0];
					partitioner = new Partitioner("");
					partitioner.setPartitioner(childRDD.getPartitioner());
					System.out.println("\nMethod: " +method+ ", childMethod: " +childMethod);
					if(CheckHelper.checkMethod(childMethod) == MethodsType.shuffle){
						if(CheckHelper.checkDependsOnPartitioner(childMethod)){
							Boolean equal = partitioner.checkIfChanges(method);
							System.out.println("Child method depends on partitioner, equal: "+ equal);
							if(!equal){
								stage = new Stage(stages++);
								stagesMap.get(mapStages.get(childRDD.getId())).addParentId(stage.getId());								
							} else stage = stagesMap.get(mapStages.get(childRDD.getId()));
						} else {
							System.out.println("Doesn't depend on partitioner, create a new stage");
							stage = new Stage(stages++);
							stagesMap.get(mapStages.get(childRDD.getId())).addParentId(stage.getId());
						}
					} else {
						stage = stagesMap.get(mapStages.get(childRDD.getId()));
					}
				} else {
					stage = new Stage(stages++);
				}
				stage.addRDD(rdd);
				System.out.println("Partitioner before: " +partitioner.getPartitioner());
				partitioner.setPartitionerOfMethod(method);
				System.out.println("Partitioner updated: " +partitioner.getPartitioner());
				
				mapStages.put(rdd.getId(), stage.getId());				
				stagesMap.put(stage.getId(), stage);
			}
		}		


		removeDuplicatedStages();
		return new PairMapInt(stagesMap, stages);
	}
	
	private static void removeDuplicatedStages(){
		List<Integer> idsOfStagesToRemove = new ArrayList<Integer>();
		List<Integer> idsOfStagesUsedToRemove = new ArrayList<Integer>();
		for (Map.Entry<Integer, Stage> entry : stagesMap.entrySet()){
			stage = entry.getValue();
			int check = CheckHelper.checkExistence(stage, stagesMap);
			if(check > -1 && !idsOfStagesUsedToRemove.contains(stage.getId())){

				Stage stageToKeep = stagesMap.get(check);
				RDD lastRDD = stageToKeep.getRDDs().get(0);
				RDD lastRDDOfTheDuplicatedStage = stage.getRDDs().get(0);
				lastRDD.addChildrenId(lastRDDOfTheDuplicatedStage.getChildrenId());
//				stageToKeep.addChildId(stage.getChildId());

				for (Map.Entry<Integer, Stage> entry2 : stagesMap.entrySet()){
					Stage stage2 = entry2.getValue();
					List<RDD> rdds = stage2.getRDDs();
					for (int z = 0; z < rdds.size(); z++) {
						if (lastRDDOfTheDuplicatedStage.getChildrenId().contains(rdds.get(z).getId())){
							rdds.get(z).getParentsId().remove(Integer.valueOf(lastRDDOfTheDuplicatedStage.getId()));
							rdds.get(z).addParentId(lastRDD.getId());
							
							stage2.getParentsIds().remove(Integer.valueOf(stage.getId()));
							stage2.addParentId(stageToKeep.getId());
						}
					}
				}	
				System.out.println("Remove: " +stage.getId()+ ", keep: " +check);
				idsOfStagesToRemove.add(stage.getId());
				idsOfStagesUsedToRemove.add(check);
			}						
		}
		for (Integer i : idsOfStagesToRemove) {
			System.out.println("Remove: " +i);
			stagesMap.remove(i);
		}

		for (Map.Entry<Integer, Stage> entry : stagesMap.entrySet()){
			stage = entry.getValue();
			List<Integer> parentsIds = stage.getParentsIds();
			System.out.println("Stage: "+stage.getId()+ ", parent: " +parentsIds);
			for(Integer id: parentsIds){
				System.out.println("Id of a child: " +id);
				Stage parent = stagesMap.get(id);
//				prettyPrint(child);
				System.out.println("ID of parent: " +stage.getId()+", to put in: " +parent.getId());
				parent.addChildId(stage.getId());
			}			
		}

		/*
		for (Map.Entry<Integer, Stage> entry : stagesMap.entrySet()){
			stage = entry.getValue();
			List<RDD> rdds = stage.getRDDs();
			for(RDD rdd: rdds)
				rdd.removeChild();
			stage.removeChilds();
		}
		*/
	}
}
