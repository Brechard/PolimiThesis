package com.polimi.thesis;

import java.util.ArrayList;
import java.util.List;

public class RDD{
	private String callSite;
	private int id;
	private List<Integer> childrenIds;
	private List<Integer> parentsIds;
	private List<String> condition;
	private List<String> loop;
	
	public RDD(String callSite, int id){
		this.callSite = callSite;
		this.id = id;
		childrenIds = new ArrayList<Integer>();
	}

	public void addChildId(int id){
		childrenIds.add(id);
	}
	
	public void addChildId(List<Integer> ids){
		childrenIds.addAll(ids);			
	}

	public void addParentId(int id){
		if(parentsIds == null)
			parentsIds = new ArrayList<Integer>();
		parentsIds.add(id);
	}
	
	public void addParentId(List<Integer> ids){
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
	
	public List<Integer> getchildrenId(){
		return childrenIds;
	}
	public List<Integer> getParentsId(){
		if(parentsIds == null)
			parentsIds = new ArrayList<Integer>();
		return parentsIds;
	}
	public void setCondition(List<String> condition){
		this.condition = condition;
	}
	public void setLoop(List<String> loop){
		this.loop = loop;
	}
}	

