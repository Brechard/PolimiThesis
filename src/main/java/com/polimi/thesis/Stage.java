package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

public class Stage{
	private int id;
	private List<RDD> rdds;
	private List<Integer> childId;
	private List<Integer> parentId;
	private Boolean updatedChild;

	public Stage(int id){
		System.out.println(id);
		this.id = id;
		childId = new ArrayList<Integer>();
		parentId = new ArrayList<Integer>();
		rdds = new ArrayList<RDD>();
	}
	public void addRDD(RDD child) {
		rdds.add(child);
	}
	public void addChildId(int id){
		childId.add(id);
	}
	public void addChildId(List<Integer> ids){
		childId.addAll(ids);
	}
	public List<Integer> getChildId(){
		return childId;
	}
	public void addParentId(int id){
		parentId.add(id);
	}
	public void addParentsIds(List<Integer> ids){
		parentId.addAll(ids);
	}
	public List<Integer> getParentsIds(){
		return parentId;
	}
	public int getId(){
		return id;
	}
	public List<RDD> getRDDs(){
		return rdds;
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
		return rdds.isEmpty();
	}	
	public void removeChilds(){
		childId = null;
	}
	public void removeParents(){
		parentId = null;
	}
	public void emptyParents(){
		parentId = new ArrayList<Integer>();
	}
}
