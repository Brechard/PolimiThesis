package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

import com.polimi.thesis.Variables.NodeType;

public class Stage{
	private int id;
	private List<Node> nodes;
	private List<Integer> childId;
	private List<Integer> parentId;
	private Boolean updatedChild;

	public Stage(int id){
//		System.out.println(id);
		this.id = id;
		childId = new ArrayList<Integer>();
		parentId = new ArrayList<Integer>();
		nodes = new ArrayList<Node>();
	}
	public void addNode(Node child) {
		nodes.add(child);
	}
	public void addChildId(int id){
		if(childId == null)
			childId = new ArrayList<Integer>();
		if(!childId.contains(id))
			childId.add(id);
	}
	public void addChildId(List<Integer> ids){
		if(childId == null)
			childId = new ArrayList<Integer>();
		List<Integer> notContained = new ArrayList<Integer>(ids);
		notContained.removeAll(childId);
		childId.addAll(notContained);
	}
	public List<Integer> getChildrenId(){
		return childId;
	}
	public void addParentId(int id){
		if(parentId == null)
			parentId = new ArrayList<Integer>();
		if(!parentId.contains(id))
			parentId.add(id);
	}
	public void addParentsIds(List<Integer> ids){
		if(parentId == null)
			parentId = new ArrayList<Integer>();
		List<Integer> notContained = new ArrayList<Integer>(ids);
		notContained.removeAll(parentId);
		parentId.addAll(notContained);
	}
	public List<Integer> getParentsIds(){
		return parentId;
	}
	public int getId(){
		return id;
	}
	public List<Node> getNodes(){
		return nodes;
	}
	public void setId(int id){
		this.id = id;
	}
	public void setUpdatedChild(Boolean update){
		updatedChild = update;
	}
	public Boolean getUpdatedChild() {
		return updatedChild;
	}
	public Boolean isEmpty(){
		return nodes.isEmpty();
	}	
	public void removeChildrenIds(){
		childId = null;
	}
	public void removeParentsIds(){
		parentId = null;
	}
	public void emptyParents(){
		parentId = new ArrayList<Integer>();
	}
	public void removeUpdated(){
		updatedChild = null;
	}
	public void addAll(Stage stage){
		List<Node> notContained = new ArrayList<Node>(stage.getNodes());
		notContained.removeAll(nodes);
		nodes.addAll(notContained);
		List<Integer> not = new ArrayList<Integer>(stage.getChildrenId());
		not.removeAll(childId);
		childId.addAll(not);
		not = new ArrayList<Integer>(stage.getParentsIds());
		not.removeAll(parentId);
		parentId.addAll(not);
	}
	public Node getNode(int id){
		for(Node node: nodes){
			if(node.getId() == id)
				return node;
		}
		return null;
	}
	public Node getNode(int id, NodeType type){
		for(Node node: nodes){
			if(node.getId() == id && node.getType() == type)
				return node;
		}
		return null;
	}
}
