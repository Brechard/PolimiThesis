package com.polimi.thesis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StageMap {
	private int id;
	private Map<Integer, Node> nodes;
	private List<Integer> parentsIds;

	public StageMap(Stage stage){
		this.id = stage.getId();
		this.parentsIds = stage.getParentsIds();
		nodes = new HashMap<Integer, Node>();
		for(Node node: stage.getNodes()){
			nodes.put(node.getId(), node);
		}
	}
	public Map<Integer, Node> getNodes(){
		return nodes;
	}
}
