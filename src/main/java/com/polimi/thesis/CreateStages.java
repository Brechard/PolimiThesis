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
		System.out.println("Stages: ");
		MainClass.prettyPrint(stagesMap2);
		stagesMap = new HashMap<Integer, Stage>();
		Partitioner partitioner = new Partitioner("");
		removeDuplicatedRDDs(stagesMap2);
		for (Map.Entry<Integer, Stage> entry : stagesMap2.entrySet()){
			stage = entry.getValue();
			List<Node> listRDDs = stage.getNodes();
			Map<Integer, Integer> mapStages = new HashMap<Integer, Integer>(); // key = rddId, value = stageId where the rdd is
			Map<Integer, Node> mapRDDs = new HashMap<Integer, Node>(); 
			for(int i = 0; i < listRDDs.size(); i++){
				mapRDDs.put(listRDDs.get(i).getId(), listRDDs.get(i));
			}
			for(int i = 0; i < listRDDs.size(); i++){
				Node rdd = listRDDs.get(i);
				String method = rdd.getCallSite().split(" at ")[0];
				MethodsType type = CheckHelper.checkMethod(method);
				List<Integer> childrenIds = rdd.getChildrenId();
				// Right now it is only possible that the rdd has one child or cero, because we haven't merged yet the stages that are equal
				if(childrenIds.size() > 0){
					Node childRDD = mapRDDs.get(childrenIds.get(0));
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
				stage.addNode(rdd);
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
	
	private static void removeDuplicatedRDDs(Map<Integer, Stage> stagesMap2){
		Map<Integer, Integer> idsOfRDDsToRemove = new HashMap<Integer, Integer>(); // key duplicated, value original
		if(stagesMap2.containsKey(0))
			stage = stagesMap2.get(0);
		else return;
		List<Node> rdds = stage.getNodes();
		for(int i = 0; i < rdds.size(); i++){
			Node rdd = rdds.get(i);
			for(int j = i + 1; j < rdds.size(); j++){
				Node rdd2 = rdds.get(j);
				if(rdd.getCallSite().equals(rdd2.getCallSite())){
					if(!idsOfRDDsToRemove.containsKey(rdd2.getId()))
						idsOfRDDsToRemove.put(rdd2.getId(), rdd.getId());
				}
			}			
		}
		for (Map.Entry<Integer, Integer> entry : idsOfRDDsToRemove.entrySet()){
			int idRemove = entry.getKey();
			int idPut = entry.getValue();
			System.out.println("idRemove: " +idRemove+", idPut: " +idPut);
		}
		if(idsOfRDDsToRemove.size() > 0){
			try {
				throw new Error("Duplicated RDDs, this should not have happened");
			} catch (Error e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.out.println("Error: " +e);
			}
		}

		for (Map.Entry<Integer, Integer> entry : idsOfRDDsToRemove.entrySet()){
			int idRemove = entry.getKey();
			int idPut = entry.getValue();
			System.out.println("idRemove: " +idRemove+", idPut: " +idPut);
			for(int j = 0; j < rdds.size(); j++){
				Node rdd = rdds.get(j);
				if(rdd.getId() == idRemove){
					rdds.remove(j);
				}				
				if(rdd.getChildrenId().contains(idRemove)){
					rdd.getChildrenId().remove((int) idRemove);
					rdd.getChildrenId().add(idPut);
				}
				if(rdd.getParentsId().contains(idRemove)){
					rdd.getParentsId().remove(new Integer(idRemove));
					rdd.getParentsId().add(idPut);
				}
			}
		}
	}
	
	private static void removeDuplicatedStages(){
		List<Integer> idsOfStagesToRemove = new ArrayList<Integer>();
		List<Integer> idsOfStagesUsedToRemove = new ArrayList<Integer>();
		for (Map.Entry<Integer, Stage> entry : stagesMap.entrySet()){
			stage = entry.getValue();
			int check = CheckHelper.checkExistence(stage, stagesMap);
			if(check > -1 && !idsOfStagesUsedToRemove.contains(stage.getId())){

				Stage stageToKeep = stagesMap.get(check);
				Node lastRDD = stageToKeep.getNodes().get(0);
				Node lastRDDOfTheDuplicatedStage = stage.getNodes().get(0);
				lastRDD.addChildrenId(lastRDDOfTheDuplicatedStage.getChildrenId());
//				stageToKeep.addChildId(stage.getChildId());

				for (Map.Entry<Integer, Stage> entry2 : stagesMap.entrySet()){
					Stage stage2 = entry2.getValue();
					List<Node> rdds = stage2.getNodes();
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
			List<Node> rdds = stage.getNodes();
			for(Node rdd: rdds)
				rdd.removeChild();
			stage.removeChilds();
		}
		*/
	}
}
