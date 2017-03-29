package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

import com.polimi.thesis.Variables.NodeType;

public class Node{
	private String callSite;
	private NodeType type;
	private int id;
	private List<Integer> childrenIds;
	private List<Integer> parentsIds;
	private List<String> loop;
	private String conditionId = null;
	private String partitioner;
	public Node(String callSite, int id, String  partitioner, NodeType type){
		this.callSite = callSite;
		this.type = type;
		this.id = id;
		childrenIds = new ArrayList<Integer>();
		parentsIds = new ArrayList<Integer>();					
		this.setPartitioner(partitioner);
	}
	/*
	 * This constructor is used only for creating joins or forks.
	 */
	public Node(int id, NodeType type){
		this.type = type;
		this.id = id;
	}

	public void addChildId(int id){
		if(childrenIds == null)
			childrenIds = new ArrayList<Integer>();
		if(!childrenIds.contains(id))
			childrenIds.add(id);
	}
	
	public void addChildrenId(List<Integer> ids){
		if(childrenIds == null)
			childrenIds = new ArrayList<Integer>();
		childrenIds.addAll(ids);			
	}

	public void addParentId(int id){
		if(parentsIds == null)
			parentsIds = new ArrayList<Integer>();
		if(!parentsIds.contains(id))
			parentsIds.add(id);
		System.out.println("Add parent ID:" +id+", in " +this.id+", type: " +this.type);
	}
	
	public void addParentsId(List<Integer> ids){
		if(parentsIds == null)
			parentsIds = new ArrayList<Integer>();
		parentsIds.addAll(ids);			
	}

	public int getId(){
		return id;
	}
	
	public String getCallSite(){
		return callSite;
	}	
	
	public List<Integer> getChildrenId(){
		return childrenIds;
	}
	public List<Integer> getParentsId(){
		if(parentsIds == null)
			parentsIds = new ArrayList<Integer>();
		return parentsIds;
	}
	public void setLoop(List<String> loop){
		this.loop = loop;
	}
	public void removeChildrenIds(){
		childrenIds = null;
	}
	public void removeParentsIds(){
		parentsIds = null;
	}
	public String getPartitioner() {
		return partitioner;
	}

	public void setPartitioner(String partitioner) {
		this.partitioner = partitioner;
	}
	
	public void removePartitioner(){
		this.partitioner = null;
	}
	public void setId(int id){
		this.id = id;
	}
	public Boolean isRDD(){
		return type == NodeType.rdd;
	}
	public NodeType getType(){
		return type;
	}
	public int getConditionId() {
		if(conditionId == null)
			return -1;
		return Integer.valueOf(conditionId);
	}
	public void setConditionId(int conditionId) {
		this.conditionId = String.valueOf(conditionId);
	}
}	


